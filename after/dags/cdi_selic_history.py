from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from br_market.http import read_text
from br_market.k8s import pod_override
from br_market.notify import MarketDataFailureNotifier
from br_market.settings import BCB_SERIES_URL, STAGING_CONN_ID, WAREHOUSE_CONN_ID
from br_market.storage import fetch_records, upsert_records
from br_market.transforms import parse_bcb_series, sync_rate_rows

RATE_COLUMNS = ("ref_date", "series_id", "rate_pct")
RATE_KEYS = ("ref_date", "series_id")
MART_COLUMNS = ("ref_date", "series_id", "rate_pct", "source_system")
MART_KEYS = ("ref_date", "series_id")
SERIES = {"CDI": "12", "SELIC": "11"}

DEFAULT_ARGS = {
    "owner": "market-data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=4),
    "on_failure_callback": MarketDataFailureNotifier(channel="bcb-rates"),
    "queue": "celery",
}


def ingest_series(series_name: str, series_id: str, fixture_name: str, *, ds=None, **_context) -> int:
    url = BCB_SERIES_URL.format(series_id=series_id)
    raw = read_text(url, fixture_name=fixture_name)
    parsed = parse_bcb_series(raw, series_id=series_name, ref_date=ds)
    return upsert_records(
        table="br_market.bcb_rates_staging",
        rows=parsed["rows"],
        columns=RATE_COLUMNS,
        key_fields=RATE_KEYS,
        postgres_conn_id=STAGING_CONN_ID,
    )


def sync_staging_to_warehouse(*, ds=None, **_context) -> int:
    staging = fetch_records(
        STAGING_CONN_ID,
        sql="SELECT ref_date, series_id, rate_pct FROM br_market.bcb_rates_staging",
        table="br_market.bcb_rates_staging",
    )
    synced = sync_rate_rows(staging, source_system="bcb_sgs")
    return upsert_records(
        table="br_market.bcb_rates_mart",
        rows=synced,
        columns=MART_COLUMNS,
        key_fields=MART_KEYS,
        postgres_conn_id=WAREHOUSE_CONN_ID,
    )


with DAG(
    dag_id="cdi_selic_history",
    description="BCB CDI and Selic ingest plus staging-to-warehouse sync",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="30 9 * * 1-5",
    catchup=False,
    tags=["market-data", "bcb", "cdi", "selic", "airflow3"],
) as dag:
    start = EmptyOperator(task_id="start")
    with TaskGroup(group_id="rate_ingest") as rate_ingest:
        PythonOperator(
            task_id="fetch_cdi",
            python_callable=ingest_series,
            op_kwargs={"series_name": "CDI", "series_id": SERIES["CDI"], "fixture_name": "bcb_cdi.json"},
            executor_config=pod_override(memory="512Mi", cpu="250m"),
            retries=3,
            retry_delay=timedelta(minutes=5),
        )
        PythonOperator(
            task_id="fetch_selic",
            python_callable=ingest_series,
            op_kwargs={"series_name": "SELIC", "series_id": SERIES["SELIC"], "fixture_name": "bcb_selic.json"},
            retries=2,
            retry_delay=timedelta(minutes=4),
        )
    sync = PythonOperator(
        task_id="sync_staging_to_warehouse",
        python_callable=sync_staging_to_warehouse,
        retries=2,
        retry_delay=timedelta(minutes=8),
    )
    finish = EmptyOperator(task_id="finish")

    start >> rate_ingest >> sync >> finish
