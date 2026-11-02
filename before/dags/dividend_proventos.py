from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.postgres_operator import PostgresOperator
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from legacy_etl.http import read_text
from legacy_etl.notify import MarketDataFailureNotifier
from legacy_etl.transforms import apply_proventos, parse_holdings, parse_proventos

DEFAULT_ARGS = {
    "owner": "market-data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": MarketDataFailureNotifier(channel="proventos"),
    "queue": "celery",
}


def events_available(**context):
    execution_date = context["execution_date"]
    return execution_date is not None


def extract_events(**context):
    execution_date = context["execution_date"]
    ref_date = execution_date.strftime("%Y-%m-%d")
    raw = read_text("https://dados.cvm.gov.br/dados/FI/DOC/INF_MENSAL/", fixture_name="proventos.csv")
    return parse_proventos(raw, ref_date=ref_date)


def apply_events(**context):
    from airflow.hooks.postgres_hook import PostgresHook

    events = context["ti"].xcom_pull(task_ids="extract_proventos")
    execution_date = context["execution_date"]
    ref_date = execution_date.strftime("%Y-%m-%d")
    holdings = parse_holdings(read_text("https://internal/holdings", fixture_name="holdings.csv"))
    applied = apply_proventos(events["rows"], holdings, ref_date=ref_date)
    hook = PostgresHook(postgres_conn_id="postgres_market")
    hook.insert_rows(
        table="br_market.proventos_aplicados",
        rows=[
            (
                r["ticker"],
                r["tipo"],
                r["ref_date"],
                r["data_ex"],
                r["data_pagamento"],
                r["cotas"],
                r["valor_por_cota"],
                r["gross_amount"],
                r["ir_amount"],
                r["net_amount"],
            )
            for r in applied["rows"]
        ],
        target_fields=[
            "ticker",
            "tipo",
            "ref_date",
            "data_ex",
            "data_pagamento",
            "cotas",
            "valor_por_cota",
            "gross_amount",
            "ir_amount",
            "net_amount",
        ],
        replace=True,
    )
    return applied["counters"]


with DAG(
    dag_id="dividend_proventos",
    description="Dividend and proventos application using contrib operators",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(7),
    schedule_interval="0 14 * * 1-5",
    catchup=False,
    tags=["market-data", "proventos", "postgres", "airflow2"],
) as dag:
    start = DummyOperator(task_id="start")
    wait_events = PythonSensor(
        task_id="wait_events",
        python_callable=events_available,
        provide_context=True,
        poke_interval=10,
        timeout=120,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )
    extract = PythonOperator(
        task_id="extract_proventos",
        python_callable=extract_events,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=6),
    )
    apply = PythonOperator(
        task_id="apply_proventos",
        python_callable=apply_events,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=10),
    )
    analyze = PostgresOperator(
        task_id="analyze_proventos",
        postgres_conn_id="postgres_market",
        sql="ANALYZE br_market.proventos_aplicados",
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    finish = DummyOperator(task_id="finish")

    start >> wait_events >> extract >> apply >> analyze >> finish
