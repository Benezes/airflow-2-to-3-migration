from __future__ import annotations

import json
import runpy
import sys
from pathlib import Path

import pendulum

from tests.conftest import CONN_FILE, LOGICAL_DATE, ROOT

REF = pendulum.datetime(2024, 6, 10, tz="America/Sao_Paulo")


def load_golden(name: str):
    path = ROOT / "tests" / "fixtures" / "golden" / name
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def patch_external_services(monkeypatch) -> None:
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    from airflow.providers.http.sensors.http import HttpSensor
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

    monkeypatch.setattr(HttpSensor, "poke", lambda self, context: True)
    monkeypatch.setattr(KubernetesPodOperator, "execute", lambda self, context=None: None)
    monkeypatch.setattr(SQLExecuteQueryOperator, "execute", lambda self, context=None: None)
    monkeypatch.setattr(SparkSubmitOperator, "execute", _run_spark_application)


def _run_spark_application(self, context=None):
    argv = [self.application, *list(self.application_args or [])]
    previous = sys.argv
    sys.argv = argv
    try:
        runpy.run_path(self.application, run_name="__main__")
    finally:
        sys.argv = previous


def dag_from_bag(dag_id: str):
    from airflow.models.dagbag import DagBag
    from tests.conftest import DAGS_FOLDER

    bag = DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)
    assert dag_id in bag.dags, bag.import_errors
    return bag.dags[dag_id]


def run_dag(dag_id: str, monkeypatch) -> None:
    patch_external_services(monkeypatch)
    dag = dag_from_bag(dag_id)
    dag.test(logical_date=REF, conn_file_path=str(CONN_FILE))
