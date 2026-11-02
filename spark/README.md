# Spark showcase

Three PySpark jobs on the same public Brazilian market fixtures as the Airflow DAGs, each submitted with `SparkSubmitOperator`.

This folder is a **portfolio section**. It is not a claim about a production Spark platform, cluster manager, or streaming stack.

| Job | Input | Output |
| --- | --- | --- |
| `cdi_selic_aggregation.py` | Daily CDI/Selic | Monthly stats + 252-day compounded factor |
| `cvm_fund_scd.py` | CVM cadastro + current dim | Deduped SCD2 dimension |
| `anbima_curve_normalize.py` | Anbima ETTJ vertices | PRE/IPCA implied inflation |

- `before/` — contrib `SparkSubmitOperator`, RDD jobs
- `after/` — `apache-airflow-providers-apache-spark`, DataFrame / Spark SQL jobs

Jobs are tested with a local `SparkSession` (`master=local[1]`). See `migrations/spark_*/MIGRATION.md` and `tests/test_spark_transforms.py`.
