from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from br_market.notify import MarketDataFailureNotifier
from br_market.settings import SPARK_CONN_ID, fixture_dir, spark_output_dir

ROOT = Path(__file__).resolve().parents[3]
APPLICATION = str(ROOT / "spark" / "after" / "jobs" / "cdi_selic_aggregation.py")

DEFAULT_ARGS = {
    "owner": "market-data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": MarketDataFailureNotifier(channel="spark-cdi-selic"),
    "queue": "celery",
}


def resolve_paths(*, ds=None, **_context) -> dict:
    fixtures = fixture_dir()
    input_path = str((fixtures / "cdi_selic_daily.csv") if fixtures else "/opt/airflow/data/cdi_selic_daily.csv")
    output_path = str(spark_output_dir() / "cdi_selic" / ds)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    return {"input": input_path, "output": output_path}


with DAG(
    dag_id="spark_cdi_selic",
    description="SparkSubmitOperator: monthly CDI/Selic aggregation from BCB series",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="0 20 * * 1-5",
    catchup=False,
    tags=["spark", "bcb", "cdi", "selic", "airflow3"],
) as dag:
    start = EmptyOperator(task_id="start")
    paths = PythonOperator(
        task_id="resolve_paths",
        python_callable=resolve_paths,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    submit = SparkSubmitOperator(
        task_id="submit_cdi_selic",
        application=APPLICATION,
        conn_id=SPARK_CONN_ID,
        name="cdi_selic_aggregation",
        verbose=False,
        application_args=[
            "--input",
            "{{ ti.xcom_pull(task_ids='resolve_paths')['input'] }}",
            "--output",
            "{{ ti.xcom_pull(task_ids='resolve_paths')['output'] }}",
        ],
        conf={"spark.ui.enabled": "false", "spark.master": "local[1]"},
        retries=2,
        retry_delay=timedelta(minutes=8),
    )
    finish = EmptyOperator(task_id="finish")

    start >> paths >> submit >> finish
