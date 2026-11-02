from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import days_ago

from legacy_etl.notify import MarketDataFailureNotifier
from legacy_etl.timetable import DualSessionTimetable
from legacy_etl.transforms import parse_anbima_ettj

DEFAULT_ARGS = {
    "owner": "market-data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=6),
    "on_failure_callback": MarketDataFailureNotifier(channel="anbima-ettj"),
    "queue": "celery",
}


def file_ready(**context):
    return True


def load_ettj(**context):
    from legacy_etl.http import read_text
    from airflow.hooks.postgres_hook import PostgresHook

    execution_date = context["execution_date"]
    ref_date = execution_date.strftime("%Y-%m-%d")
    raw = read_text("https://www.anbima.com.br/informacoes/est-termo/", fixture_name="anbima_ettj.csv")
    parsed = parse_anbima_ettj(raw, ref_date=ref_date)
    hook = PostgresHook(postgres_conn_id="postgres_market")
    hook.insert_rows(
        table="br_market.anbima_ettj",
        rows=[(r["ref_date"], r["curva"], r["vertice_dias"], r["taxa"]) for r in parsed["rows"]],
        target_fields=["ref_date", "curva", "vertice_dias", "taxa"],
        replace=True,
    )
    return parsed["counters"]


with DAG(
    dag_id="anbima_yield_curves",
    description="Anbima yield curves with sensors and a custom dual-session timetable",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(5),
    timetable=DualSessionTimetable(),
    catchup=False,
    tags=["market-data", "anbima", "sensor", "airflow2"],
) as dag:
    start = DummyOperator(task_id="start")
    wait_http = HttpSensor(
        task_id="wait_anbima_http",
        http_conn_id="anbima_http",
        endpoint="/informacoes/est-termo/ettj",
        poke_interval=15,
        timeout=120,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )
    wait_payload = PythonSensor(
        task_id="wait_payload_date",
        python_callable=file_ready,
        provide_context=True,
        poke_interval=5,
        timeout=60,
    )
    load = PythonOperator(
        task_id="upsert_ettj",
        python_callable=load_ettj,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=7),
    )
    validate_pod = KubernetesPodOperator(
        task_id="validate_in_pod",
        name="anbima-validate",
        image="python:3.10-slim",
        cmds=["python", "-c", "print('anbima-ok')"],
        in_cluster=False,
        get_logs=True,
        retries=1,
        retry_delay=timedelta(minutes=10),
    )
    analyze = PostgresOperator(
        task_id="analyze_anbima_ettj",
        postgres_conn_id="postgres_market",
        sql="ANALYZE br_market.anbima_ettj",
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    finish = DummyOperator(task_id="finish")

    start >> wait_http >> wait_payload >> load >> validate_pod >> analyze >> finish
