from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, length, lit, regexp_replace, row_number
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

from br_market.money import money

SNAPSHOT_SCHEMA = StructType(
    [
        StructField("CNPJ_FUNDO", StringType(), True),
        StructField("DENOM_SOCIAL", StringType(), True),
        StructField("SIT", StringType(), True),
        StructField("TP_FUNDO", StringType(), True),
        StructField("CLASSE", StringType(), True),
        StructField("DT_REG", StringType(), True),
        StructField("VL_PATRIM_LIQ", StringType(), True),
    ]
)
DIM_SCHEMA = StructType(
    [
        StructField("cnpj", StringType(), False),
        StructField("denom_social", StringType(), False),
        StructField("classe", StringType(), False),
        StructField("sit", StringType(), False),
        StructField("dt_reg", StringType(), False),
        StructField("vl_patrim_liq", StringType(), False),
        StructField("valid_from", StringType(), False),
        StructField("valid_to", StringType(), False),
        StructField("is_current", StringType(), False),
    ]
)
OPEN_VALID_TO = "9999-12-31"
ACTIVE = "EM FUNCIONAMENTO NORMAL"


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


def normalize_snapshot(frame: DataFrame) -> DataFrame:
    cleaned = (
        frame.withColumn("cnpj", regexp_replace(col("CNPJ_FUNDO"), r"\D", ""))
        .withColumn("denom_social", col("DENOM_SOCIAL"))
        .withColumn("classe", col("CLASSE"))
        .withColumn("sit", col("SIT"))
        .withColumn("dt_reg", col("DT_REG"))
        .withColumn("vl_patrim_liq", col("VL_PATRIM_LIQ"))
        .where(length(col("cnpj")) == 14)
        .where(col("sit") == ACTIVE)
    )
    ranked = cleaned.withColumn(
        "rn",
        row_number().over(Window.partitionBy("cnpj").orderBy(col("dt_reg").desc(), col("denom_social").desc())),
    )
    return ranked.where(col("rn") == 1).drop("rn")


def scd2(snapshot: DataFrame, current_dim: DataFrame, ref_date: str) -> list[dict]:
    incoming = normalize_snapshot(snapshot).select(
        "cnpj", "denom_social", "classe", "sit", "dt_reg", "vl_patrim_liq"
    )
    current = current_dim.where(col("is_current") == "Y")
    compared = incoming.alias("n").join(current.alias("c"), on="cnpj", how="left")
    changed = compared.where(
        col("c.cnpj").isNull()
        | (col("n.denom_social") != col("c.denom_social"))
        | (col("n.classe") != col("c.classe"))
        | (col("n.sit") != col("c.sit"))
    )
    unchanged_cnpj = compared.where(
        col("c.cnpj").isNotNull()
        & (col("n.denom_social") == col("c.denom_social"))
        & (col("n.classe") == col("c.classe"))
        & (col("n.sit") == col("c.sit"))
    ).select(col("n.cnpj").alias("cnpj"))
    closed = (
        current.join(changed.select(col("n.cnpj").alias("cnpj")), on="cnpj", how="inner")
        .withColumn("valid_to", lit(ref_date))
        .withColumn("is_current", lit("N"))
        .select("cnpj", "denom_social", "classe", "sit", "dt_reg", "vl_patrim_liq", "valid_from", "valid_to", "is_current")
    )
    opened = changed.select(
        col("n.cnpj").alias("cnpj"),
        col("n.denom_social").alias("denom_social"),
        col("n.classe").alias("classe"),
        col("n.sit").alias("sit"),
        col("n.dt_reg").alias("dt_reg"),
        col("n.vl_patrim_liq").alias("vl_patrim_liq"),
        lit(ref_date).alias("valid_from"),
        lit(OPEN_VALID_TO).alias("valid_to"),
        lit("Y").alias("is_current"),
    )
    untouched = (
        current.join(unchanged_cnpj, on="cnpj", how="inner")
        .join(incoming.select("cnpj", col("vl_patrim_liq").alias("new_pl")), on="cnpj", how="inner")
        .withColumn("vl_patrim_liq", col("new_pl"))
        .drop("new_pl")
        .select(
            "cnpj",
            "denom_social",
            "classe",
            "sit",
            "dt_reg",
            "vl_patrim_liq",
            "valid_from",
            "valid_to",
            "is_current",
        )
    )
    history = current_dim.where(col("is_current") == "N")
    result = history.unionByName(closed).unionByName(opened).unionByName(untouched)
    rows = []
    for record in result.orderBy("cnpj", "valid_from", "is_current").collect():
        rows.append(
            {
                "cnpj": record["cnpj"],
                "denom_social": record["denom_social"],
                "classe": record["classe"],
                "sit": record["sit"],
                "dt_reg": record["dt_reg"],
                "vl_patrim_liq": money(record["vl_patrim_liq"]),
                "valid_from": record["valid_from"],
                "valid_to": record["valid_to"],
                "is_current": record["is_current"],
            }
        )
    return rows


def run(spark: SparkSession, snapshot_path: str, dim_path: str, output_path: str, ref_date: str) -> list[dict]:
    snapshot = (
        spark.read.option("header", True).option("delimiter", ";").schema(SNAPSHOT_SCHEMA).csv(snapshot_path)
    )
    current_dim = spark.read.option("header", True).schema(DIM_SCHEMA).csv(dim_path)
    rows = scd2(snapshot, current_dim, ref_date)
    spark.createDataFrame(rows).coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--dim", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--ref-date", required=True)
    args = parser.parse_args()
    spark = local_session("cvm_fund_scd")
    try:
        run(spark, args.snapshot, args.dim, args.output, args.ref_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
