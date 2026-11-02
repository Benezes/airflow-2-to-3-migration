import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession

_SRC = Path(__file__).resolve().parents[3] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from legacy_etl.money import money
from legacy_etl.transforms import normalize_cnpj

ACTIVE = "EM FUNCIONAMENTO NORMAL"
OPEN_VALID_TO = "9999-12-31"


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


def _parse_snapshot_line(line):
    parts = line.split(";")
    return {
        "cnpj": normalize_cnpj(parts[0]),
        "denom_social": parts[1].strip(),
        "sit": parts[2].strip(),
        "classe": parts[4].strip(),
        "dt_reg": parts[5].strip(),
        "vl_patrim_liq": parts[6].strip(),
    }


def transform_scd(snapshot_lines, dim_lines, ref_date):
    snap_header = snapshot_lines.first()
    snapshot = (
        snapshot_lines.filter(lambda line: line != snap_header)
        .map(_parse_snapshot_line)
        .filter(lambda row: row["sit"] == ACTIVE)
        .map(lambda row: (row["cnpj"], row))
        .reduceByKey(lambda left, right: left if left["dt_reg"] >= right["dt_reg"] else right)
        .map(lambda item: item[1])
        .collect()
    )
    dim_header = dim_lines.first()
    dim_rows = (
        dim_lines.filter(lambda line: line != dim_header)
        .map(lambda line: line.split(","))
        .map(
            lambda parts: {
                "cnpj": parts[0],
                "denom_social": parts[1],
                "classe": parts[2],
                "sit": parts[3],
                "dt_reg": parts[4],
                "vl_patrim_liq": parts[5],
                "valid_from": parts[6],
                "valid_to": parts[7],
                "is_current": parts[8],
            }
        )
        .collect()
    )
    current = {row["cnpj"]: row for row in dim_rows if row["is_current"] == "Y"}
    history = [row for row in dim_rows if row["is_current"] == "N"]
    result = list(history)
    incoming_cnpj = set()
    for row in snapshot:
        incoming_cnpj.add(row["cnpj"])
        prev = current.get(row["cnpj"])
        changed = prev is None or (
            prev["denom_social"] != row["denom_social"]
            or prev["classe"] != row["classe"]
            or prev["sit"] != row["sit"]
        )
        if changed:
            if prev is not None:
                closed = dict(prev)
                closed["valid_to"] = ref_date
                closed["is_current"] = "N"
                result.append(closed)
            opened = {
                "cnpj": row["cnpj"],
                "denom_social": row["denom_social"],
                "classe": row["classe"],
                "sit": row["sit"],
                "dt_reg": row["dt_reg"],
                "vl_patrim_liq": money(row["vl_patrim_liq"]),
                "valid_from": ref_date,
                "valid_to": OPEN_VALID_TO,
                "is_current": "Y",
            }
            result.append(opened)
        else:
            kept = dict(prev)
            kept["vl_patrim_liq"] = money(row["vl_patrim_liq"])
            result.append(kept)
    for row in result:
        row["vl_patrim_liq"] = money(row["vl_patrim_liq"])
    result.sort(key=lambda item: (item["cnpj"], item["valid_from"], item["is_current"]))
    return result


def run(spark, snapshot_path, dim_path, output_path, ref_date):
    snapshot_lines = spark.sparkContext.textFile(snapshot_path)
    dim_lines = spark.sparkContext.textFile(dim_path)
    rows = transform_scd(snapshot_lines, dim_lines, ref_date)
    header = spark.sparkContext.parallelize(
        ["cnpj,denom_social,classe,sit,dt_reg,vl_patrim_liq,valid_from,valid_to,is_current"]
    )
    body = spark.sparkContext.parallelize(
        [
            ",".join(
                [
                    r["cnpj"],
                    r["denom_social"],
                    r["classe"],
                    r["sit"],
                    r["dt_reg"],
                    r["vl_patrim_liq"],
                    r["valid_from"],
                    r["valid_to"],
                    r["is_current"],
                ]
            )
            for r in rows
        ]
    )
    header.union(body).coalesce(1).saveAsTextFile(output_path)
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--dim", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--ref-date", required=True)
    args = parser.parse_args()
    spark = local_session("cvm_fund_scd_legacy")
    try:
        run(spark, args.snapshot, args.dim, args.output, args.ref_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
