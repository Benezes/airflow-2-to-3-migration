from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from br_market.http import read_text
from br_market.k8s import pod_override
from br_market.notify import MarketDataFailureNotifier
from br_market.storage import publish_json, upsert_records
from br_market.transforms import parse_b3_indicators

B3_COLUMNS = ("code", "ref_date", "close", "prev_close", "daily_return")
B3_KEYS = ("code", "ref_date")
B3_FIXTURE = "b3_indicators.json"


@dag(
    dag_id="b3_indicators",
    description="B3 index levels with TaskFlow XCom and S3 publish",
    start_date=datetime(2024, 1, 1),
    schedule="0 11 * * 1-5",
    catchup=False,
    default_args={
        "owner": "market-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "on_failure_callback": MarketDataFailureNotifier(channel="b3-indicators"),
        "queue": "celery",
    },
    tags=["market-data", "b3", "s3", "airflow3"],
)
def b3_indicators():
    @task(task_id="fetch_indicators", retries=2, retry_delay=timedelta(minutes=3))
    def fetch_indicators() -> dict:
        from airflow.sdk import get_current_context

        context = get_current_context()
        logical_date = context["logical_date"]
        ref_date = logical_date.date().isoformat()
        payload = read_text("https://sistemaswebb3-listados.b3.com.br/indices", fixture_name=B3_FIXTURE)
        return json.loads(payload) | {"ref_date": ref_date}

    @task(task_id="transform_returns", retries=1, retry_delay=timedelta(minutes=2))
    def transform_returns(raw: dict) -> dict:
        return parse_b3_indicators(raw, ref_date=raw["ref_date"])

    @task(
        task_id="publish_and_upsert",
        retries=2,
        retry_delay=timedelta(minutes=5),
        executor_config=pod_override(memory="768Mi", cpu="400m"),
    )
    def publish_and_upsert(parsed: dict) -> dict:
        rows = parsed["rows"]
        ref_date = rows[0]["ref_date"] if rows else None
        key = f"b3/indicators/{ref_date}.json"
        location = publish_json(key, json.dumps(rows, sort_keys=True))
        upserted = upsert_records(
            table="br_market.b3_indicators",
            rows=rows,
            columns=B3_COLUMNS,
            key_fields=B3_KEYS,
        )
        return {"s3": location, "upserted": upserted, "counters": parsed["counters"]}

    publish_and_upsert(transform_returns(fetch_indicators()))


b3_indicators()
