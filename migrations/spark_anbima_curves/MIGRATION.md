# Spark: Anbima PRE / IPCA implied inflation

## What broke

Airflow 2 used contrib `SparkSubmitOperator` and a job that split `;` lines on an RDD, then joined PRE and IPCA vertices in Python dicts. `registerTempTable` / `saveAsTextFile` belong to the Spark 2.x surface.

## Why

Spark 3.x replaced `registerTempTable` with `createOrReplaceTempView` and made DataFrame/SQL the supported API. The Airflow provider move is the same contrib → `apache-spark` provider change as the other jobs.

## What changed

- CSV reader with delimiter and schema
- PRE ⋈ IPCA on `vertice_dias` via Spark SQL
- `implied_inflation = PRE - IPCA` at 8 decimal places

Golden file: `tests/fixtures/golden/spark_anbima_curves.json`.
