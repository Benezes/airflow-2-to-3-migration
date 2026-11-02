# Spark: CDI / Selic monthly aggregation

## What broke

The Airflow 2 DAG imported `SparkSubmitOperator` from `airflow.contrib.operators.spark_submit_operator`, used `schedule_interval`, `days_ago`, `provide_context`, and `execution_date`. The job itself was an RDD pipeline: `textFile` → `map`/`filter` → Python-side aggregation → `saveAsTextFile`.

## Why

`airflow.contrib` is gone; SparkSubmit lives in `apache-airflow-providers-apache-spark`. On the job side, RDD-first code is the Spark 1.x/2.x style. Spark 3.x / 4.x expects DataFrame operations (`groupBy`, `Window`, `exp`/`log` for compounding) and structured writers.

## What changed

- Operator import → `airflow.providers.apache.spark.operators.spark_submit.SparkSubmitOperator`
- Job reads a typed CSV schema
- Monthly grain with `n_obs`, min/max/avg/last rate, and a 252-business-day compounded factor
- Writer is `DataFrame.write.csv` instead of `saveAsTextFile`

Output is identical to the RDD job on `tests/fixtures/cdi_selic_daily.csv` (see `spark_cdi_selic.json`).
