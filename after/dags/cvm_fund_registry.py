from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from br_market.http import read_text
from br_market.k8s import pod_override
from br_market.notify import MarketDataFailureNotifier
from br_market.settings import CVM_CADASTRO_URL, POSTGRES_CONN_ID
from br_market.storage import upsert_records
from br_market.transforms import parse_cvm_cadastro

CVM_COLUMNS = (
    "cnpj",
    "denom_social",
    "classe",
    "sit",
    "dt_reg",
    "vl_patrim_liq",
    "ref_date",
)
CVM_KEYS = ("cnpj", "ref_date")

DEFAULT_ARGS = {
    "owner": "market-data",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": MarketDataFailureNotifier(channel="cvm-funds"),
    "queue": "celery",
}


def extract_cadastro(*, logical_date=None, ds=None, **_context) -> str:
    ref_date = logical_date.date().isoformat() if logical_date is not None else ds
    raw = read_text(CVM_CADASTRO_URL, fixture_name="cvm_cad_fi.csv")
    parsed = parse_cvm_cadastro(raw, ref_date=ref_date)
    return parsed


def load_cadastro(payload: dict, **_context) -> int:
    rows = payload["rows"]
    return upsert_records(
        table="br_market.cvm_fundos",
        rows=rows,
        columns=CVM_COLUMNS,
        key_fields=CVM_KEYS,
    )


with DAG(
    dag_id="cvm_fund_registry",
    description="CVM fund registry extract, normalize, and Postgres upsert",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="0 9 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["market-data", "cvm", "postgres", "airflow3"],
) as dag:
    start = EmptyOperator(task_id="start")
    extract = PythonOperator(
        task_id="extract_cadastro",
        python_callable=extract_cadastro,
        executor_config=pod_override(memory="1Gi", cpu="500m"),
        retries=3,
        retry_delay=timedelta(minutes=8),
    )
    load = PythonOperator(
        task_id="upsert_postgres",
        python_callable=load_cadastro,
        op_kwargs={"payload": extract.output},
        retries=2,
        retry_delay=timedelta(minutes=4),
    )
    analyze = SQLExecuteQueryOperator(
        task_id="analyze_cvm_fundos",
        conn_id=POSTGRES_CONN_ID,
        sql="ANALYZE br_market.cvm_fundos",
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    finish = EmptyOperator(task_id="finish")

    start >> extract >> load >> analyze >> finish
