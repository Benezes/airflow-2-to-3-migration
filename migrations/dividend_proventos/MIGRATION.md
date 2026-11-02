# Dividend / proventos application

## What broke

This DAG was a catalog of 2.x leftovers: `schedule_interval`, `days_ago`, `execution_date`, `provide_context=True`, `DummyOperator`, `airflow.contrib.operators.postgres_operator.PostgresOperator`, and `airflow.contrib.sensors.python_sensor.PythonSensor`.

Any one of those is enough for AIR301. Together they make the file unimportable on Airflow 3.

## Why

Contrib was emptied before 3.0; remaining shims were removed instead of being carried. Context keys and DAG arguments were renamed to match the Task SDK.

## What changed

- contrib Postgres operator → `SQLExecuteQueryOperator`
- contrib Python sensor dropped; the pipeline is extract → apply → analyze
- JCP withholding stays at 15%; dividends and FII rendimentos stay gross = net
- Holdings application output is unchanged and locked by `tests/fixtures/golden/proventos_applied.json`
