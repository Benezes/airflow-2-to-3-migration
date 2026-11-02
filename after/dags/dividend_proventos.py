from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from br_market.http import read_text
from br_market.notify import MarketDataFailureNotifier
from br_market.settings import POSTGRES_CONN_ID
from br_market.storage import upsert_records
from br_market.transforms import apply_proventos, parse_holdings, parse_proventos

EVENT_COLUMNS = ("ticker", "tipo", "data_ex", "data_pagamento", "valor_por_cota")
EVENT_KEYS = ("ticker", "data_ex", "tipo")
APPLY_COLUMNS = (
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
)
APPLY_KEYS = ("ticker", "data_ex", "tipo")

DEFAULT_ARGS = {
    "owner": "market-data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": MarketDataFailureNotifier(channel="proventos"),
    "queue": "celery",
}


def extract_events(*, ds=None, **_context) -> dict:
    raw = read_text("https://dados.cvm.gov.br/dados/FI/DOC/INF_MENSAL/", fixture_name="proventos.csv")
    return parse_proventos(raw, ref_date=ds)


def apply_events(events: dict, *, ds=None, **_context) -> dict:
    holdings_raw = read_text("https://internal/holdings", fixture_name="holdings.csv")
    holdings = parse_holdings(holdings_raw)
    applied = apply_proventos(events["rows"], holdings, ref_date=ds)
    upsert_records(
        table="br_market.proventos_eventos",
        rows=events["rows"],
        columns=EVENT_COLUMNS,
        key_fields=EVENT_KEYS,
    )
    upsert_records(
        table="br_market.proventos_aplicados",
        rows=applied["rows"],
        columns=APPLY_COLUMNS,
        key_fields=APPLY_KEYS,
    )
    return applied


with DAG(
    dag_id="dividend_proventos",
    description="Apply cash proventos (dividendo, JCP, rendimento) onto fund holdings",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="0 14 * * 1-5",
    catchup=False,
    tags=["market-data", "proventos", "postgres", "airflow3"],
) as dag:
    start = EmptyOperator(task_id="start")
    extract = PythonOperator(
        task_id="extract_proventos",
        python_callable=extract_events,
        retries=2,
        retry_delay=timedelta(minutes=6),
    )
    apply = PythonOperator(
        task_id="apply_proventos",
        python_callable=apply_events,
        op_kwargs={"events": extract.output},
        retries=1,
        retry_delay=timedelta(minutes=10),
    )
    analyze = SQLExecuteQueryOperator(
        task_id="analyze_proventos",
        conn_id=POSTGRES_CONN_ID,
        sql="ANALYZE br_market.proventos_aplicados",
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    finish = EmptyOperator(task_id="finish")

    start >> extract >> apply >> analyze >> finish
