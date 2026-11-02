import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession

_SRC = Path(__file__).resolve().parents[3] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from legacy_etl.money import money, rate8, to_decimal


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


def transform_aggregation(lines):
    header = lines.first()
    records = (
        lines.filter(lambda line: line != header)
        .map(lambda line: line.split(","))
        .map(lambda parts: (parts[0], parts[1], float(parts[2])))
        .collect()
    )
    grouped = {}
    for ref_date, series_id, rate in records:
        year_month = ref_date[:7]
        key = (year_month, series_id)
        grouped.setdefault(key, []).append((ref_date, rate))
    rows = []
    for (year_month, series_id), items in grouped.items():
        items.sort(key=lambda item: item[0])
        rates = [to_decimal(item[1]) for item in items]
        factor = to_decimal("1")
        for rate in rates:
            daily = (to_decimal("1") + rate / to_decimal("100")) ** (to_decimal("1") / to_decimal("252"))
            factor *= daily
        rows.append(
            {
                "year_month": year_month,
                "series_id": series_id,
                "n_obs": len(rates),
                "avg_rate_pct": money(sum(rates) / len(rates)),
                "min_rate_pct": money(min(rates)),
                "max_rate_pct": money(max(rates)),
                "last_rate_pct": money(rates[-1]),
                "compounded_factor": rate8(factor),
            }
        )
    rows.sort(key=lambda item: (item["year_month"], item["series_id"]))
    return rows


def run(spark, input_path, output_path):
    lines = spark.sparkContext.textFile(input_path)
    rows = transform_aggregation(lines)
    header = spark.sparkContext.parallelize(
        ["year_month,series_id,n_obs,avg_rate_pct,min_rate_pct,max_rate_pct,last_rate_pct,compounded_factor"]
    )
    body = spark.sparkContext.parallelize(
        [
            ",".join(
                [
                    r["year_month"],
                    r["series_id"],
                    str(r["n_obs"]),
                    r["avg_rate_pct"],
                    r["min_rate_pct"],
                    r["max_rate_pct"],
                    r["last_rate_pct"],
                    r["compounded_factor"],
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
    args = parser.parse_args()
    spark = local_session("cdi_selic_aggregation_legacy")
    try:
        run(spark, args.input, args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
