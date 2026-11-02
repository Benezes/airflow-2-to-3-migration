from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from legacy_etl.notify import MarketDataFailureNotifier

ROOT = Path(__file__).resolve().parents[3]
APPLICATION = str(ROOT / "spark" / "before" / "jobs" / "cdi_selic_aggregation.py")

DEFAULT_ARGS = {
    "owner": "market-data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": MarketDataFailureNotifier(channel="spark-cdi-selic"),
    "queue": "celery",
}


def resolve_paths(**context):
    execution_date = context["execution_date"]
    return {"input": "/tmp/cdi_selic_daily.csv", "output": "/tmp/spark/cdi/" + execution_date.strftime("%Y-%m-%d")}


with DAG(
    dag_id="spark_cdi_selic",
    description="Legacy SparkSubmitOperator DAG for CDI/Selic aggregation",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(3),
    schedule_interval="0 20 * * 1-5",
    catchup=False,
    tags=["spark", "bcb", "airflow2"],
) as dag:
    start = DummyOperator(task_id="start")
    paths = PythonOperator(
        task_id="resolve_paths",
        python_callable=resolve_paths,
        provide_context=True,
    )
    submit = SparkSubmitOperator(
        task_id="submit_cdi_selic",
        application=APPLICATION,
        conn_id="spark_default",
        name="cdi_selic_aggregation",
        application_args=["--input", "/tmp/cdi_selic_daily.csv", "--output", "/tmp/spark/cdi"],
        retries=2,
        retry_delay=timedelta(minutes=8),
    )
    finish = DummyOperator(task_id="finish")

    start >> paths >> submit >> finish
