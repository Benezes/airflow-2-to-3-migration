# Spark: CVM fund registry dedup and SCD type 2

## What broke

Same Airflow-side breakage as the other Spark DAGs (contrib `SparkSubmitOperator`, `schedule_interval`, `execution_date`). The job used `reduceByKey` for CNPJ dedup and in-Python SCD2 against collected dicts.

## Why

Dedup and slowly changing dimensions are DataFrame problems: window `row_number` for the latest CNPJ row, `unionByName` for closed/opened/untouched sets. RDD `reduceByKey` plus `collect` hides the grain and does not scale.

## What changed

- Snapshot parsed with an explicit schema including `TP_FUNDO` so CSV columns line up
- Dedup: `row_number()` over `cnpj` ordered by `dt_reg`
- SCD2: close current row when `denom_social` / `classe` / `sit` change; type-1 in-place update for `vl_patrim_liq` when the descriptive key is unchanged
- Historical `is_current=N` rows are preserved via `unionByName`

Golden file: `tests/fixtures/golden/spark_cvm_scd.json`.
