from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from legacy_etl.notify import MarketDataFailureNotifier

ROOT = Path(__file__).resolve().parents[3]
APPLICATION = str(ROOT / "spark" / "before" / "jobs" / "anbima_curve_normalize.py")

DEFAULT_ARGS = {
    "owner": "market-data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=7),
    "on_failure_callback": MarketDataFailureNotifier(channel="spark-anbima"),
    "queue": "celery",
}


def resolve_paths(**context):
    execution_date = context["execution_date"]
    return {"input": "/tmp/anbima_ettj.csv", "output": "/tmp/spark/anbima/" + execution_date.strftime("%Y-%m-%d")}


with DAG(
    dag_id="spark_anbima_curves",
    description="Legacy SparkSubmitOperator DAG for Anbima curve normalize",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(3),
    schedule_interval="0 21 * * 1-5",
    catchup=False,
    tags=["spark", "anbima", "airflow2"],
) as dag:
    start = DummyOperator(task_id="start")
    paths = PythonOperator(
        task_id="resolve_paths",
        python_callable=resolve_paths,
        provide_context=True,
    )
    submit = SparkSubmitOperator(
        task_id="submit_anbima_curves",
        application=APPLICATION,
        conn_id="spark_default",
        name="anbima_curve_normalize",
        application_args=["--input", "/tmp/anbima_ettj.csv", "--output", "/tmp/spark/anbima", "--ref-date", "{{ ds }}"],
        retries=2,
        retry_delay=timedelta(minutes=9),
    )
    finish = DummyOperator(task_id="finish")

    start >> paths >> submit >> finish
