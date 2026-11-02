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
APPLICATION = str(ROOT / "spark" / "after" / "jobs" / "anbima_curve_normalize.py")

DEFAULT_ARGS = {
    "owner": "market-data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=7),
    "on_failure_callback": MarketDataFailureNotifier(channel="spark-anbima"),
    "queue": "celery",
}


def resolve_paths(*, ds=None, **_context) -> dict:
    fixtures = fixture_dir()
    input_path = str((fixtures / "anbima_ettj.csv") if fixtures else "/opt/airflow/data/anbima_ettj.csv")
    output_path = str(spark_output_dir() / "anbima_curves" / ds)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    return {"input": input_path, "output": output_path, "ref_date": ds}


with DAG(
    dag_id="spark_anbima_curves",
    description="SparkSubmitOperator: Anbima PRE vs IPCA implied inflation",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="0 21 * * 1-5",
    catchup=False,
    tags=["spark", "anbima", "airflow3"],
) as dag:
    start = EmptyOperator(task_id="start")
    paths = PythonOperator(
        task_id="resolve_paths",
        python_callable=resolve_paths,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    submit = SparkSubmitOperator(
        task_id="submit_anbima_curves",
        application=APPLICATION,
        conn_id=SPARK_CONN_ID,
        name="anbima_curve_normalize",
        verbose=False,
        application_args=[
            "--input",
            "{{ ti.xcom_pull(task_ids='resolve_paths')['input'] }}",
            "--output",
            "{{ ti.xcom_pull(task_ids='resolve_paths')['output'] }}",
            "--ref-date",
            "{{ ds }}",
        ],
        conf={"spark.ui.enabled": "false", "spark.master": "local[1]"},
        retries=2,
        retry_delay=timedelta(minutes=9),
    )
    finish = EmptyOperator(task_id="finish")

    start >> paths >> submit >> finish
