import argparse

from pyspark.sql import SparkSession

from legacy_etl.money import rate8
from legacy_etl.transforms import implied_inflation


def local_session(app_name):
    return (
        SparkSession.builder.master("local[1]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def transform_curves(lines, ref_date):
    header = lines.first()
    parsed = (
        lines.filter(lambda line: line != header)
        .map(lambda line: line.split(";"))
        .filter(lambda parts: parts[0] == ref_date)
        .map(lambda parts: (parts[1].strip().upper(), int(parts[2]), float(parts[3])))
        .collect()
    )
    pre = {vertice: taxa for curva, vertice, taxa in parsed if curva == "PRE"}
    ipca = {vertice: taxa for curva, vertice, taxa in parsed if curva == "IPCA"}
    rows = []
    for vertice in sorted(set(pre).intersection(ipca)):
        pre_rate = rate8(pre[vertice])
        ipca_rate = rate8(ipca[vertice])
        rows.append(
            {
                "ref_date": ref_date,
                "vertice_dias": vertice,
                "taxa_pre": pre_rate,
                "taxa_ipca": ipca_rate,
                "implied_inflation": implied_inflation(pre_rate, ipca_rate),
            }
        )
    return rows


def run(spark, input_path, output_path, ref_date):
    lines = spark.sparkContext.textFile(input_path)
    rows = transform_curves(lines, ref_date)
    header = spark.sparkContext.parallelize(
        ["ref_date,vertice_dias,taxa_pre,taxa_ipca,implied_inflation"]
    )
    body = spark.sparkContext.parallelize(
        [
            ",".join(
                [
                    r["ref_date"],
                    str(r["vertice_dias"]),
                    r["taxa_pre"],
                    r["taxa_ipca"],
                    r["implied_inflation"],
                ]
            )
            for r in rows
        ]
    )
    header.union(body).coalesce(1).saveAsTextFile(output_path)
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--ref-date", required=True)
    args = parser.parse_args()
    spark = local_session("anbima_curve_normalize_legacy")
    try:
        run(spark, args.input, args.output, args.ref_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
