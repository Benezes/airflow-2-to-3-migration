from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window

_SRC = Path(__file__).resolve().parents[3] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from pyspark.sql.functions import avg, col, count, exp, log, lit, max as spark_max, min as spark_min, row_number, sum as spark_sum
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from br_market.money import money, rate8

SCHEMA = StructType(
    [
        StructField("ref_date", StringType(), False),
        StructField("series_id", StringType(), False),
        StructField("rate_pct", DoubleType(), False),
    ]
)


def local_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.master("local[1]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def transform_aggregation(frame: DataFrame) -> list[dict]:
    enriched = (
        frame.withColumn("year_month", col("ref_date").substr(1, 7))
        .withColumn("daily_factor", exp(log(lit(1.0) + col("rate_pct") / lit(100.0)) / lit(252.0)))
    )
    latest = Window.partitionBy("year_month", "series_id").orderBy(col("ref_date").desc())
    last_rate = (
        enriched.withColumn("rn", row_number().over(latest))
        .where(col("rn") == 1)
        .select("year_month", "series_id", col("rate_pct").alias("last_rate_pct"))
    )
    aggregated = (
        enriched.groupBy("year_month", "series_id")
        .agg(
            count(lit(1)).alias("n_obs"),
            avg("rate_pct").alias("avg_rate_pct"),
            spark_min("rate_pct").alias("min_rate_pct"),
            spark_max("rate_pct").alias("max_rate_pct"),
            exp(spark_sum(log("daily_factor"))).alias("compounded_factor"),
        )
        .join(last_rate, on=["year_month", "series_id"], how="inner")
        .orderBy("year_month", "series_id")
    )
    rows = []
    for record in aggregated.collect():
        rows.append(
            {
                "year_month": record["year_month"],
                "series_id": record["series_id"],
                "n_obs": int(record["n_obs"]),
                "avg_rate_pct": money(record["avg_rate_pct"]),
                "min_rate_pct": money(record["min_rate_pct"]),
                "max_rate_pct": money(record["max_rate_pct"]),
                "last_rate_pct": money(record["last_rate_pct"]),
                "compounded_factor": rate8(record["compounded_factor"]),
            }
        )
    return rows


def run(spark: SparkSession, input_path: str, output_path: str) -> list[dict]:
    frame = spark.read.option("header", True).schema(SCHEMA).csv(input_path)
    rows = transform_aggregation(frame)
    spark.createDataFrame(rows).coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    spark = local_session("cdi_selic_aggregation")
    try:
        run(spark, args.input, args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
