from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.conftest import AIRFLOW_HOME, LOGICAL_DATE
from tests.helpers import run_dag


AFTER_DAGS = [
    "cvm_fund_registry",
    "b3_indicators",
    "anbima_yield_curves",
    "cdi_selic_history",
    "dividend_proventos",
    "spark_cdi_selic",
    "spark_cvm_scd",
    "spark_anbima_curves",
]


@pytest.mark.parametrize("dag_id", AFTER_DAGS)
def test_after_dag_test_runs(dag_id: str, monkeypatch, reset_store: Path) -> None:
    run_dag(dag_id, monkeypatch)


def test_cvm_execution_wrote_store(monkeypatch, reset_store: Path) -> None:
    run_dag("cvm_fund_registry", monkeypatch)
    payload = json.loads((reset_store / "br_market_cvm_fundos.json").read_text(encoding="utf-8"))
    assert {row["cnpj"] for row in payload} == {"12345678000190", "11222333000144"}
    assert all(row["ref_date"] == LOGICAL_DATE for row in payload)


def test_b3_execution_wrote_s3_and_store(monkeypatch, reset_store: Path) -> None:
    run_dag("b3_indicators", monkeypatch)
    s3_object = AIRFLOW_HOME / "s3" / "b3" / "indicators" / f"{LOGICAL_DATE}.json"
    assert s3_object.exists()
    rows = json.loads(s3_object.read_text(encoding="utf-8"))
    assert {row["code"] for row in rows} == {"IBOV", "IFIX", "SMLL"}


def test_proventos_execution_wrote_applied_cash(monkeypatch, reset_store: Path) -> None:
    run_dag("dividend_proventos", monkeypatch)
    applied = json.loads((reset_store / "br_market_proventos_aplicados.json").read_text(encoding="utf-8"))
    by_ticker = {row["ticker"]: row for row in applied}
    assert by_ticker["PETR4"]["net_amount"] == "500.00"
    assert by_ticker["VALE3"]["net_amount"] == "340.00"
    assert by_ticker["HGLG11"]["net_amount"] == "184.00"
