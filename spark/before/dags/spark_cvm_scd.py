from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from legacy_etl.notify import MarketDataFailureNotifier

ROOT = Path(__file__).resolve().parents[3]
APPLICATION = str(ROOT / "spark" / "before" / "jobs" / "cvm_fund_scd.py")

DEFAULT_ARGS = {
    "owner": "market-data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=6),
    "on_failure_callback": MarketDataFailureNotifier(channel="spark-cvm-scd"),
    "queue": "celery",
}


def resolve_paths(**context):
    execution_date = context["execution_date"]
    ref_date = execution_date.strftime("%Y-%m-%d")
    return {"snapshot": "/tmp/cvm_cad_fi.csv", "dim": "/tmp/cvm_dim_current.csv", "ref_date": ref_date}


with DAG(
    dag_id="spark_cvm_scd",
    description="Legacy SparkSubmitOperator DAG for CVM fund SCD",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(3),
    schedule_interval="30 20 * * 1-5",
    catchup=False,
    tags=["spark", "cvm", "airflow2"],
) as dag:
    start = DummyOperator(task_id="start")
    paths = PythonOperator(
        task_id="resolve_paths",
        python_callable=resolve_paths,
        provide_context=True,
    )
    submit = SparkSubmitOperator(
        task_id="submit_cvm_scd",
        application=APPLICATION,
        conn_id="spark_default",
        name="cvm_fund_scd",
        application_args=[
            "--snapshot",
            "/tmp/cvm_cad_fi.csv",
            "--dim",
            "/tmp/cvm_dim_current.csv",
            "--output",
            "/tmp/spark/cvm",
            "--ref-date",
            "{{ ds }}",
        ],
        retries=2,
        retry_delay=timedelta(minutes=9),
    )
    finish = DummyOperator(task_id="finish")

    start >> paths >> submit >> finish
