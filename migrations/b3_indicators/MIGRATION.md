# B3 indicators

## What broke

The TaskFlow DAG declared `schedule_interval`, `days_ago`, and a task parameter named `execution_date`. It also pulled `yesterday_ds` from kwargs, used `airflow.hooks.S3_hook.S3Hook` and `airflow.hooks.postgres_hook.PostgresHook`, and imported `@dag` / `@task` from `airflow.decorators`.

Ruff AIR301 flags `execution_date` on `@task` functions. `yesterday_ds` is gone from task context. The old S3/Postgres hook paths are removed or moved.

## Why

Airflow 3 deleted several templated context keys (`execution_date`, `yesterday_ds`, `next_ds`, …) because they were ambiguous once data intervals and `logical_date` were split. Decorators and DAG/task entry points moved to `airflow.sdk`. Amazon and Postgres APIs live in providers.

## What changed

- `@dag` / `@task` / `get_current_context` from `airflow.sdk`
- TaskFlow XCom is unchanged: fetch → transform → publish
- S3 publish uses `S3Hook` from `apache-airflow-providers-amazon`, with a local filesystem fallback for tests
- Upsert uses the same row contract as Airflow 2 (`code`, `ref_date`, `close`, `prev_close`, `daily_return`)

Daily return math is identical to the Airflow 2 transform and compared against `tests/fixtures/golden/b3_indicators.json`.
