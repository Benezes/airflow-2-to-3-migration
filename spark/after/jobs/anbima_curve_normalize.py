from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, upper
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from br_market.money import rate8
from br_market.transforms import implied_inflation

CURVE_SCHEMA = StructType(
    [
        StructField("data", StringType(), False),
        StructField("curva", StringType(), False),
        StructField("vertice_dias", IntegerType(), False),
        StructField("taxa", DoubleType(), False),
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


def transform_curves(frame: DataFrame, ref_date: str) -> list[dict]:
    filtered = frame.where(col("data") == ref_date).withColumn("curva", upper(col("curva")))
    filtered.createOrReplaceTempView("ettj")
    joined = filtered.sparkSession.sql(
        """
        SELECT
            p.vertice_dias AS vertice_dias,
            p.taxa AS taxa_pre,
            i.taxa AS taxa_ipca
        FROM ettj p
        INNER JOIN ettj i
          ON p.vertice_dias = i.vertice_dias
        WHERE p.curva = 'PRE' AND i.curva = 'IPCA'
        ORDER BY p.vertice_dias
        """
    )
    rows = []
    for record in joined.collect():
        pre_rate = rate8(record["taxa_pre"])
        ipca_rate = rate8(record["taxa_ipca"])
        rows.append(
            {
                "ref_date": ref_date,
                "vertice_dias": int(record["vertice_dias"]),
                "taxa_pre": pre_rate,
                "taxa_ipca": ipca_rate,
                "implied_inflation": implied_inflation(pre_rate, ipca_rate),
            }
        )
    return rows


def run(spark: SparkSession, input_path: str, output_path: str, ref_date: str) -> list[dict]:
    frame = spark.read.option("header", True).option("delimiter", ";").schema(CURVE_SCHEMA).csv(input_path)
    rows = transform_curves(frame, ref_date)
    spark.createDataFrame(rows).coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--ref-date", required=True)
    args = parser.parse_args()
    spark = local_session("anbima_curve_normalize")
    try:
        run(spark, args.input, args.output, args.ref_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
