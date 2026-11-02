from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG
from airflow.timetables.trigger import MultipleCronTriggerTimetable

from br_market.http import read_text
from br_market.notify import MarketDataFailureNotifier
from br_market.settings import ANBIMA_HTTP_ENDPOINT, HTTP_CONN_ID, POSTGRES_CONN_ID, landing_dir
from br_market.storage import upsert_records
from br_market.transforms import parse_anbima_ettj

ANBIMA_COLUMNS = ("ref_date", "curva", "vertice_dias", "taxa")
ANBIMA_KEYS = ("ref_date", "curva", "vertice_dias")

DEFAULT_ARGS = {
    "owner": "market-data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=6),
    "on_failure_callback": MarketDataFailureNotifier(channel="anbima-ettj"),
    "queue": "celery",
}


def curve_path(ds: str) -> str:
    return str(landing_dir() / "anbima" / ds / "ettj.csv")


def file_is_for_date(**context) -> bool:
    ds = context["ds"]
    path = curve_path(ds)
    with open(path, encoding="utf-8") as handle:
        return ds in handle.read()


def load_ettj(*, ds=None, **_context) -> int:
    raw = read_text("https://www.anbima.com.br/informacoes/est-termo/", fixture_name="anbima_ettj.csv")
    parsed = parse_anbima_ettj(raw, ref_date=ds)
    return upsert_records(
        table="br_market.anbima_ettj",
        rows=parsed["rows"],
        columns=ANBIMA_COLUMNS,
        key_fields=ANBIMA_KEYS,
    )


with DAG(
    dag_id="anbima_yield_curves",
    description="Anbima PRE/IPCA yield curves with sensors and Kubernetes validation",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule=MultipleCronTriggerTimetable(
        "0 8 * * 1-5",
        "0 18 * * 1-5",
        timezone="America/Sao_Paulo",
    ),
    catchup=False,
    tags=["market-data", "anbima", "sensor", "airflow3"],
) as dag:
    start = EmptyOperator(task_id="start")
    wait_http = HttpSensor(
        task_id="wait_anbima_http",
        http_conn_id=HTTP_CONN_ID,
        endpoint=ANBIMA_HTTP_ENDPOINT,
        poke_interval=15,
        timeout=120,
        mode="poke",
        retries=1,
        retry_delay=timedelta(minutes=2),
    )
    wait_file = FileSensor(
        task_id="wait_ettj_file",
        filepath=str(landing_dir() / "anbima" / "{{ ds }}" / "ettj.csv"),
        poke_interval=5,
        timeout=90,
        mode="poke",
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    wait_payload = PythonSensor(
        task_id="wait_payload_date",
        python_callable=file_is_for_date,
        poke_interval=5,
        timeout=60,
        mode="poke",
    )
    load = PythonOperator(
        task_id="upsert_ettj",
        python_callable=load_ettj,
        retries=2,
        retry_delay=timedelta(minutes=7),
    )
    validate_pod = KubernetesPodOperator(
        task_id="validate_in_pod",
        name="anbima-validate",
        image="python:3.12-slim",
        cmds=["python", "-c", "print('anbima-ok')"],
        kubernetes_conn_id="kubernetes_default",
        in_cluster=False,
        get_logs=True,
        on_finish_action="delete_pod",
        retries=1,
        retry_delay=timedelta(minutes=10),
    )
    analyze = SQLExecuteQueryOperator(
        task_id="analyze_anbima_ettj",
        conn_id=POSTGRES_CONN_ID,
        sql="ANALYZE br_market.anbima_ettj",
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    finish = EmptyOperator(task_id="finish")

    start >> wait_http >> wait_file >> wait_payload >> load >> validate_pod >> analyze >> finish
