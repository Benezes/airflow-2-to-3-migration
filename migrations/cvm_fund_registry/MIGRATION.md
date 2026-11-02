# CVM fund registry

## What broke

The Airflow 2 DAG used `PythonOperator` from `airflow.operators.python_operator`, `provide_context=True`, `context["execution_date"]`, `DummyOperator`, `days_ago`, `schedule_interval`, `PostgresHook` from `airflow.hooks.postgres_hook`, and `KubernetesPodOperator` from `airflow.contrib`.

Airflow 3 removed those import paths, the `provide_context` flag, `execution_date`, `schedule_interval`, `days_ago`, and the entire `airflow.contrib` package. The DAG would not parse.

## Why

Airflow 3 split core operators into `apache-airflow-providers-standard`, moved database operators/hooks into provider packages, and replaced the old execution-date vocabulary with `logical_date` / `schedule`. Contrib code had already been deprecated through 2.x.

## What changed

- `schedule_interval` → `schedule`
- `days_ago(...)` → a static `datetime(2024, 1, 1)`
- `DummyOperator` → `EmptyOperator`
- `PythonOperator` from the standard provider; context is injected without `provide_context`
- `execution_date` → `logical_date`
- Postgres writes go through `PostgresHook.insert_rows` (replace/upsert) plus `SQLExecuteQueryOperator` for `ANALYZE`
- Failure handling moved from a callable class to `airflow.sdk.BaseNotifier`
- `executor_config` `pod_override` is unchanged and still models Celery/Kubernetes worker sizing

Business logic (CNPJ normalize, active-fund filter, counters) is unchanged and locked by golden files.
