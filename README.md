# Airflow 2 → Airflow 3 migration showcase

[![CI](https://github.com/Benezes/airflow-2-to-3-migration/actions/workflows/ci.yml/badge.svg)](https://github.com/Benezes/airflow-2-to-3-migration/actions/workflows/ci.yml)

Fixed-price migration work for small US and EU data teams that still run Airflow 2 and need a boring, testable path onto Airflow 3.

This repository is a public before/after portfolio. It uses **public Brazilian financial market data** only (CVM fund registry, B3 index levels, Anbima yield curves, BCB CDI/Selic, cash proventos). There are no client names, no vendor lock-in stories, and no private schemas.

The Spark jobs in `spark/` are a **first-class showcase** of PySpark 3.x/4.x plus `SparkSubmitOperator` on the same datasets. They are not presented as anyone’s production cluster.

## Problem

Airflow 3 removes APIs that most 2.x DAG files still use:

- `schedule_interval`, `days_ago`, `execution_date`, `provide_context`
- `airflow.contrib.*`, `SubDagOperator`, `DummyOperator`
- core-package operators that now live in providers (`standard`, `postgres`, `http`, `amazon`, `cncf-kubernetes`, `apache-spark`)
- Task context keys (`yesterday_ds`, `next_ds`, …)
- Import paths that Ruff rules `AIR301` / `AIR302` / `AIR311` / `AIR312` flag

Teams that “just upgrade the image” watch DAG import fail on the first scheduler heartbeat. The business transforms are rarely the problem. The Airflow surface is.

## Approach

1. Inventory every DAG against Ruff AIR3 rules (the same check Airflow documents for 3.x upgrades).
2. Keep transform code pure Python / PySpark so outputs can be golden-tested without a cluster.
3. Rewrite the orchestration layer onto Airflow 3 (`airflow.sdk`, providers, `TaskGroup`, `BaseNotifier`, `MultipleCronTriggerTimetable`).
4. Prove the rewrite: DagBag import on Airflow 3.1, structure assertions, `dag.test()` with local/mocked I/O, Spark unit tests on a local `SparkSession`, regression against frozen fixtures.

`before/` is intentionally broken on Airflow 3. `after/` is what you parse, test, and run.

## Layout

```
before/dags/                 Airflow 2 DAGs (do not import on 3.x)
after/dags/                  Migrated Airflow 3 DAGs
spark/before/                Legacy SparkSubmitOperator DAGs + RDD jobs
spark/after/                 Airflow 3 SparkSubmitOperator DAGs + DataFrame jobs
src/legacy_etl/              Original transform logic (golden comparison)
src/br_market/               Migrated transform logic
migrations/<pipeline>/MIGRATION.md
tests/fixtures/              Public-style sample files + golden outputs
```

### Airflow pipelines

| DAG | Theme | 2.x pattern migrated |
| --- | --- | --- |
| `cvm_fund_registry` | CVM `cad_fi` registry → Postgres upsert | `PythonOperator` + `provide_context` + old imports, `pod_override`, retries |
| `b3_indicators` | B3 index levels, daily return, S3 publish | TaskFlow + XCom, old S3/Postgres hooks |
| `anbima_yield_curves` | Anbima PRE/IPCA ETTJ | Sensors, custom multi-cron timetable, `KubernetesPodOperator` |
| `cdi_selic_history` | BCB CDI & Selic, staging → warehouse | `SubDagOperator` → `TaskGroup`, DB-to-DB sync |
| `dividend_proventos` | Dividendo / JCP / rendimento onto holdings | `schedule_interval`, `execution_date`, `airflow.contrib` |

Shared production-like defaults: `depends_on_past=False`, Celery `queue`, retries 1–3 with 1–10 minute delays, `requests.HTTPAdapter(max_retries=...)` on HTTP, bulk upsert via `PostgresHook.insert_rows`, task counters in XCom payloads, `on_failure_callback` migrated to `BaseNotifier`.

### Spark pipelines

| DAG / job | Theme | Spark API move |
| --- | --- | --- |
| `spark_cdi_selic` | Monthly CDI/Selic aggregation, 252-day compounding | RDD `textFile`/`map` → DataFrame `groupBy` + `Window` |
| `spark_cvm_scd` | Fund registry dedup + SCD type 2 | `reduceByKey` → `row_number` + `unionByName` |
| `spark_anbima_curves` | PRE vs IPCA implied inflation | Line split + dict join → Spark SQL |

Orchestration is `SparkSubmitOperator` from `apache-airflow-providers-apache-spark`. Jobs run in tests with `master=local[1]` and do not require Docker, YARN, or a remote cluster.

## Automated checks

GitHub Actions (`.github/workflows/ci.yml`) on every push and pull request:

- Ruff `AIR301,AIR302,AIR311,AIR312` on `after/`, `spark/after/`, and `src/br_market/` (must be clean)
- A test that `before/` and `spark/before/` **do** emit AIR3 findings
- DagBag import of every migrated DAG on Airflow **3.1.3**, zero import errors
- Structure: task ids, upstream edges, `schedule`, `catchup=False`, tags
- `dag.test()` for each migrated DAG with local store/S3/landing dirs; Kubernetes, HTTP poke, SQL `ANALYZE`, and Spark-submit are stubbed to local implementations
- Regression: migrated transforms and Spark jobs match `tests/fixtures/golden/*` and the original logic
- PySpark unit tests on a local `SparkSession`

Pinned versions: see `requirements.txt` (Airflow 3.1.3 + official `constraints-3.12.txt`) and `requirements-dev.txt`.

## How to run

Python 3.12, Java 21 (for PySpark 4.0.1), and a virtualenv:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.3/constraints-3.12.txt
pip install -r requirements-dev.txt
ruff check after spark/after src/br_market --select AIR301,AIR302,AIR311,AIR312
pytest -q
```

Optional UI demo (not used by tests):

```bash
cp .env.example .env
mkdir -p logs config
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_IMAGE_NAME=apache/airflow:3.1.3" >> .env
docker compose up --build
```

Compose starts Airflow 3.1.3 (CeleryExecutor) + Postgres 17 + Redis. Change `_AIRFLOW_WWW_USER_PASSWORD` and `AIRFLOW__API_AUTH__JWT_SECRET` before exposing the port. Spark-submit inside the compose image is **not** wired; use `pytest` for job execution.

## Services

_Owner: replace this section with engagement terms, rate card, and contact._

- Airflow 2.7+ → 3.x DAG inventory, Ruff AIR3 cleanup, and provider import rewrites
- Pytest harness: DagBag, `dag.test()`, golden outputs, CI
- PySpark 2.x RDD → 3.x/4.x DataFrame jobs orchestrated by `SparkSubmitOperator`
- Postgres upsert / DB-to-DB cutovers for small warehouses

## License

Code in this repository is provided as a public technical sample. Public market data schemas follow CVM, B3, Anbima, and BCB field names; fixtures are synthetic rows for tests, not a live feed.
