from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago

from legacy_etl.http import read_text
from legacy_etl.notify import MarketDataFailureNotifier
from legacy_etl.transforms import parse_bcb_series, sync_rate_rows

DEFAULT_ARGS = {
    "owner": "market-data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=4),
    "on_failure_callback": MarketDataFailureNotifier(channel="bcb-rates"),
    "queue": "celery",
}


def ingest_series(series_name, series_id, fixture_name, **context):
    from airflow.hooks.postgres_hook import PostgresHook

    execution_date = context["execution_date"]
    ref_date = execution_date.strftime("%Y-%m-%d")
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.%s/dados" % series_id
    raw = read_text(url, fixture_name=fixture_name)
    parsed = parse_bcb_series(raw, series_id=series_name, ref_date=ref_date)
    hook = PostgresHook(postgres_conn_id="postgres_staging")
    hook.insert_rows(
        table="br_market.bcb_rates_staging",
        rows=[(r["ref_date"], r["series_id"], r["rate_pct"]) for r in parsed["rows"]],
        target_fields=["ref_date", "series_id", "rate_pct"],
        replace=True,
    )
    return parsed["counters"]


def sync_staging_to_warehouse(**context):
    from airflow.hooks.postgres_hook import PostgresHook

    src = PostgresHook(postgres_conn_id="postgres_staging")
    dst = PostgresHook(postgres_conn_id="postgres_warehouse")
    records = src.get_records("SELECT ref_date, series_id, rate_pct FROM br_market.bcb_rates_staging")
    rows = [{"ref_date": r[0], "series_id": r[1], "rate_pct": r[2]} for r in records]
    synced = sync_rate_rows(rows, source_system="bcb_sgs")
    dst.insert_rows(
        table="br_market.bcb_rates_mart",
        rows=[(r["ref_date"], r["series_id"], r["rate_pct"], r["source_system"]) for r in synced],
        target_fields=["ref_date", "series_id", "rate_pct", "source_system"],
        replace=True,
    )
    return len(synced)


def build_ingest_subdag(parent_dag_id, child_dag_id, args):
    subdag = DAG(
        dag_id="%s.%s" % (parent_dag_id, child_dag_id),
        default_args=args,
        schedule_interval=None,
        start_date=days_ago(2),
        catchup=False,
    )
    with subdag:
        PythonOperator(
            task_id="fetch_cdi",
            python_callable=ingest_series,
            op_kwargs={"series_name": "CDI", "series_id": "12", "fixture_name": "bcb_cdi.json"},
            provide_context=True,
            retries=3,
            retry_delay=timedelta(minutes=5),
        )
        PythonOperator(
            task_id="fetch_selic",
            python_callable=ingest_series,
            op_kwargs={"series_name": "SELIC", "series_id": "11", "fixture_name": "bcb_selic.json"},
            provide_context=True,
            retries=2,
            retry_delay=timedelta(minutes=4),
        )
    return subdag


with DAG(
    dag_id="cdi_selic_history",
    description="CDI/Selic history with SubDagOperator and DB-to-DB sync",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(14),
    schedule_interval="30 9 * * 1-5",
    catchup=False,
    tags=["market-data", "bcb", "cdi", "selic", "airflow2"],
) as dag:
    start = DummyOperator(task_id="start")
    ingest = SubDagOperator(
        task_id="rate_ingest",
        subdag=build_ingest_subdag("cdi_selic_history", "rate_ingest", DEFAULT_ARGS),
        default_args=DEFAULT_ARGS,
        retries=1,
        retry_delay=timedelta(minutes=8),
    )
    sync = PythonOperator(
        task_id="sync_staging_to_warehouse",
        python_callable=sync_staging_to_warehouse,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=8),
    )
    finish = DummyOperator(task_id="finish")

    start >> ingest >> sync >> finish
