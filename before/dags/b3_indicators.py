from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from legacy_etl.http import read_text
from legacy_etl.notify import MarketDataFailureNotifier
from legacy_etl.transforms import parse_b3_indicators


@dag(
    dag_id="b3_indicators",
    description="B3 indicators TaskFlow with Airflow 2 XCom and schedule_interval",
    start_date=days_ago(3),
    schedule_interval="0 11 * * 1-5",
    catchup=False,
    default_args={
        "owner": "market-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "on_failure_callback": MarketDataFailureNotifier(channel="b3-indicators"),
        "queue": "celery",
    },
    tags=["market-data", "b3", "s3", "airflow2"],
)
def b3_indicators():
    @task(task_id="fetch_indicators")
    def fetch_indicators(execution_date=None, **kwargs):
        ref_date = execution_date.strftime("%Y-%m-%d")
        payload = read_text(
            "https://sistemaswebb3-listados.b3.com.br/indices",
            fixture_name="b3_indicators.json",
        )
        import json

        data = json.loads(payload)
        data["ref_date"] = ref_date
        return data

    @task(task_id="transform_returns")
    def transform_returns(raw, execution_date=None, **kwargs):
        yesterday = kwargs.get("yesterday_ds")
        _ = yesterday
        return parse_b3_indicators(raw, ref_date=raw["ref_date"])

    @task(task_id="publish_and_upsert")
    def publish_and_upsert(parsed, **kwargs):
        from airflow.hooks.S3_hook import S3Hook
        from airflow.hooks.postgres_hook import PostgresHook
        import json

        rows = parsed["rows"]
        hook = S3Hook(aws_conn_id="aws_default")
        hook.load_string(
            json.dumps(rows),
            key="b3/indicators/%s.json" % rows[0]["ref_date"],
            bucket_name="br-market-public",
            replace=True,
        )
        pg = PostgresHook(postgres_conn_id="postgres_market")
        pg.insert_rows(
            table="br_market.b3_indicators",
            rows=[
                (r["code"], r["ref_date"], r["close"], r["prev_close"], r["daily_return"]) for r in rows
            ],
            target_fields=["code", "ref_date", "close", "prev_close", "daily_return"],
            replace=True,
        )
        return parsed["counters"]

    publish_and_upsert(transform_returns(fetch_indicators()))


b3_indicators()
