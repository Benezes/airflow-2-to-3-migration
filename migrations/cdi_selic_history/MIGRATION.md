# CDI / Selic history

## What broke

`SubDagOperator` and `airflow.operators.subdag` were deleted. Nested DAG parsing, a separate scheduler slot, and `parent.child` DAG ids are gone. The ingest of BCB series 12 (CDI) and 11 (Selic) lived in a sub-DAG; the parent then ran a DB-to-DB sync with two `PostgresHook` connections.

## Why

SubDAGs were a known performance and UI problem in 2.x. Airflow 3 keeps grouping inside a single DAG via `TaskGroup`, and cross-DAG dependency belongs on Assets when it is actually a separate pipeline.

## What changed

- `SubDagOperator` → `TaskGroup(group_id="rate_ingest")` with `fetch_cdi` and `fetch_selic`
- Task ids become `rate_ingest.fetch_cdi` / `rate_ingest.fetch_selic`
- Staging → warehouse sync is the same row mapping (`source_system="bcb_sgs"`)
- `schedule_interval` → `schedule`, `provide_context` removed

The sync contract is covered by `tests/fixtures/golden/bcb_rates_mart.json`.
