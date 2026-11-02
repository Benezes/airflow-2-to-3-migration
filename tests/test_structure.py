from __future__ import annotations

from tests.helpers import dag_from_bag

EXPECTED = {
    "cvm_fund_registry": {
        "task_ids": {"start", "extract_cadastro", "upsert_postgres", "analyze_cvm_fundos", "finish"},
        "schedule": "0 9 * * 1-5",
        "catchup": False,
        "tags": {"market-data", "cvm", "postgres", "airflow3"},
        "upstream": {
            "extract_cadastro": {"start"},
            "upsert_postgres": {"extract_cadastro"},
            "analyze_cvm_fundos": {"upsert_postgres"},
            "finish": {"analyze_cvm_fundos"},
        },
    },
    "b3_indicators": {
        "task_ids": {"fetch_indicators", "transform_returns", "publish_and_upsert"},
        "schedule": "0 11 * * 1-5",
        "catchup": False,
        "tags": {"market-data", "b3", "s3", "airflow3"},
        "upstream": {
            "transform_returns": {"fetch_indicators"},
            "publish_and_upsert": {"transform_returns"},
        },
    },
    "anbima_yield_curves": {
        "task_ids": {
            "start",
            "wait_anbima_http",
            "wait_ettj_file",
            "wait_payload_date",
            "upsert_ettj",
            "validate_in_pod",
            "analyze_anbima_ettj",
            "finish",
        },
        "catchup": False,
        "tags": {"market-data", "anbima", "sensor", "airflow3"},
        "upstream": {
            "wait_anbima_http": {"start"},
            "wait_ettj_file": {"wait_anbima_http"},
            "wait_payload_date": {"wait_ettj_file"},
            "upsert_ettj": {"wait_payload_date"},
            "validate_in_pod": {"upsert_ettj"},
            "analyze_anbima_ettj": {"validate_in_pod"},
            "finish": {"analyze_anbima_ettj"},
        },
    },
    "cdi_selic_history": {
        "task_ids": {"start", "rate_ingest.fetch_cdi", "rate_ingest.fetch_selic", "sync_staging_to_warehouse", "finish"},
        "schedule": "30 9 * * 1-5",
        "catchup": False,
        "tags": {"market-data", "bcb", "cdi", "selic", "airflow3"},
        "upstream": {
            "rate_ingest.fetch_cdi": {"start"},
            "rate_ingest.fetch_selic": {"start"},
            "sync_staging_to_warehouse": {"rate_ingest.fetch_cdi", "rate_ingest.fetch_selic"},
            "finish": {"sync_staging_to_warehouse"},
        },
    },
    "dividend_proventos": {
        "task_ids": {"start", "extract_proventos", "apply_proventos", "analyze_proventos", "finish"},
        "schedule": "0 14 * * 1-5",
        "catchup": False,
        "tags": {"market-data", "proventos", "postgres", "airflow3"},
        "upstream": {
            "extract_proventos": {"start"},
            "apply_proventos": {"extract_proventos"},
            "analyze_proventos": {"apply_proventos"},
            "finish": {"analyze_proventos"},
        },
    },
    "spark_cdi_selic": {
        "task_ids": {"start", "resolve_paths", "submit_cdi_selic", "finish"},
        "schedule": "0 20 * * 1-5",
        "catchup": False,
        "tags": {"spark", "bcb", "cdi", "selic", "airflow3"},
        "upstream": {
            "resolve_paths": {"start"},
            "submit_cdi_selic": {"resolve_paths"},
            "finish": {"submit_cdi_selic"},
        },
    },
    "spark_cvm_scd": {
        "task_ids": {"start", "resolve_paths", "submit_cvm_scd", "finish"},
        "schedule": "30 20 * * 1-5",
        "catchup": False,
        "tags": {"spark", "cvm", "scd", "airflow3"},
        "upstream": {
            "resolve_paths": {"start"},
            "submit_cvm_scd": {"resolve_paths"},
            "finish": {"submit_cvm_scd"},
        },
    },
    "spark_anbima_curves": {
        "task_ids": {"start", "resolve_paths", "submit_anbima_curves", "finish"},
        "schedule": "0 21 * * 1-5",
        "catchup": False,
        "tags": {"spark", "anbima", "airflow3"},
        "upstream": {
            "resolve_paths": {"start"},
            "submit_anbima_curves": {"resolve_paths"},
            "finish": {"submit_anbima_curves"},
        },
    },
}


def test_migrated_dag_structure() -> None:
    for dag_id, expected in EXPECTED.items():
        dag = dag_from_bag(dag_id)
        assert dag.catchup is expected["catchup"]
        assert set(dag.task_ids) == expected["task_ids"]
        assert expected["tags"].issubset(set(dag.tags))
        if "schedule" in expected:
            assert str(dag.schedule) == expected["schedule"] or dag.timetable.summary == expected["schedule"]
        for task_id, upstream in expected["upstream"].items():
            task = dag.get_task(task_id)
            assert {item.task_id for item in task.upstream_list} == upstream
        assert dag.default_args.get("depends_on_past") is False
        assert dag.default_args.get("retries") in {1, 2, 3}


def test_anbima_uses_multi_cron_timetable() -> None:
    dag = dag_from_bag("anbima_yield_curves")
    assert dag.timetable.__class__.__name__ == "MultipleCronTriggerTimetable"


def test_spark_dags_use_spark_submit_operator() -> None:
    for dag_id, task_id in (
        ("spark_cdi_selic", "submit_cdi_selic"),
        ("spark_cvm_scd", "submit_cvm_scd"),
        ("spark_anbima_curves", "submit_anbima_curves"),
    ):
        dag = dag_from_bag(dag_id)
        task = dag.get_task(task_id)
        assert task.__class__.__name__ == "SparkSubmitOperator"
