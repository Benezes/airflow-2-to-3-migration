from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

from legacy_etl.http import read_text
from legacy_etl.notify import MarketDataFailureNotifier
from legacy_etl.transforms import parse_cvm_cadastro

CVM_URL = "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"

DEFAULT_ARGS = {
    "owner": "market-data",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": MarketDataFailureNotifier(channel="cvm-funds"),
    "queue": "celery",
}

POD_OVERRIDE = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={"memory": "512Mi", "cpu": "250m"},
                        limits={"memory": "1Gi", "cpu": "500m"},
                    ),
                )
            ]
        )
    )
}


def extract_cadastro(**context):
    execution_date = context["execution_date"]
    ref_date = execution_date.strftime("%Y-%m-%d")
    raw = read_text(CVM_URL, fixture_name="cvm_cad_fi.csv")
    parsed = parse_cvm_cadastro(raw, ref_date=ref_date)
    context["ti"].xcom_push(key="counters", value=parsed["counters"])
    return parsed["rows"]


def load_cadastro(**context):
    rows = context["ti"].xcom_pull(task_ids="extract_cadastro")
    hook = PostgresHook(postgres_conn_id="postgres_market")
    tuples = [
        (
            row["cnpj"],
            row["denom_social"],
            row["classe"],
            row["sit"],
            row["dt_reg"],
            row["vl_patrim_liq"],
            row["ref_date"],
        )
        for row in rows
    ]
    hook.insert_rows(
        table="br_market.cvm_fundos",
        rows=tuples,
        target_fields=[
            "cnpj",
            "denom_social",
            "classe",
            "sit",
            "dt_reg",
            "vl_patrim_liq",
            "ref_date",
        ],
        replace=True,
        replace_index=["cnpj", "ref_date"],
    )
    return len(rows)


with DAG(
    dag_id="cvm_fund_registry",
    description="CVM fund registry extract using Airflow 2 PythonOperator",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(7),
    schedule_interval="0 9 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["market-data", "cvm", "postgres", "airflow2"],
) as dag:
    start = DummyOperator(task_id="start")
    extract = PythonOperator(
        task_id="extract_cadastro",
        python_callable=extract_cadastro,
        provide_context=True,
        executor_config=POD_OVERRIDE,
        retries=3,
        retry_delay=timedelta(minutes=8),
    )
    load = PythonOperator(
        task_id="upsert_postgres",
        python_callable=load_cadastro,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=4),
    )
    sidecar = KubernetesPodOperator(
        task_id="sidecar_validate",
        name="cvm-validate",
        image="python:3.10-slim",
        cmds=["python", "-c", "print('cvm-ok')"],
        in_cluster=False,
        get_logs=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )
    finish = DummyOperator(task_id="finish")

    start >> extract >> load >> sidecar >> finish
