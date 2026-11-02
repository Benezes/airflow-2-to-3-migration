from __future__ import annotations

from airflow.models.dagbag import DagBag

from tests.conftest import DAGS_FOLDER

EXPECTED_DAG_IDS = {
    "cvm_fund_registry",
    "b3_indicators",
    "anbima_yield_curves",
    "cdi_selic_history",
    "dividend_proventos",
    "spark_cdi_selic",
    "spark_cvm_scd",
    "spark_anbima_curves",
}


def test_after_dags_import_on_airflow_3_with_zero_errors() -> None:
    bag = DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    assert EXPECTED_DAG_IDS <= set(bag.dags)
    assert len(bag.dags) >= len(EXPECTED_DAG_IDS)
