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
APPLICATION = str(ROOT / "spark" / "after" / "jobs" / "cvm_fund_scd.py")

DEFAULT_ARGS = {
    "owner": "market-data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=6),
    "on_failure_callback": MarketDataFailureNotifier(channel="spark-cvm-scd"),
    "queue": "celery",
}


def resolve_paths(*, ds=None, **_context) -> dict:
    fixtures = fixture_dir()
    snapshot = str((fixtures / "cvm_cad_fi.csv") if fixtures else "/opt/airflow/data/cvm_cad_fi.csv")
    dim = str((fixtures / "cvm_dim_current.csv") if fixtures else "/opt/airflow/data/cvm_dim_current.csv")
    output_path = str(spark_output_dir() / "cvm_scd" / ds)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    return {"snapshot": snapshot, "dim": dim, "output": output_path, "ref_date": ds}


with DAG(
    dag_id="spark_cvm_scd",
    description="SparkSubmitOperator: CVM fund registry dedup and SCD type 2",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="30 20 * * 1-5",
    catchup=False,
    tags=["spark", "cvm", "scd", "airflow3"],
) as dag:
    start = EmptyOperator(task_id="start")
    paths = PythonOperator(
        task_id="resolve_paths",
        python_callable=resolve_paths,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    submit = SparkSubmitOperator(
        task_id="submit_cvm_scd",
        application=APPLICATION,
        conn_id=SPARK_CONN_ID,
        name="cvm_fund_scd",
        verbose=False,
        application_args=[
            "--snapshot",
            "{{ ti.xcom_pull(task_ids='resolve_paths')['snapshot'] }}",
            "--dim",
            "{{ ti.xcom_pull(task_ids='resolve_paths')['dim'] }}",
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
