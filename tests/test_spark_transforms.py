from __future__ import annotations

from spark.after.jobs.anbima_curve_normalize import CURVE_SCHEMA, transform_curves
from spark.after.jobs.cdi_selic_aggregation import SCHEMA as RATE_SCHEMA
from spark.after.jobs.cdi_selic_aggregation import transform_aggregation
from spark.after.jobs.cvm_fund_scd import DIM_SCHEMA, SNAPSHOT_SCHEMA, scd2
from spark.before.jobs.anbima_curve_normalize import transform_curves as legacy_curves
from spark.before.jobs.cdi_selic_aggregation import transform_aggregation as legacy_rates
from spark.before.jobs.cvm_fund_scd import transform_scd as legacy_scd
from tests.conftest import FIXTURES, LOGICAL_DATE
from tests.helpers import load_golden


def test_cdi_selic_aggregation_matches_golden(spark) -> None:
    frame = spark.read.option("header", True).schema(RATE_SCHEMA).csv(str(FIXTURES / "cdi_selic_daily.csv"))
    migrated = transform_aggregation(frame)
    legacy = legacy_rates(spark.sparkContext.textFile(str(FIXTURES / "cdi_selic_daily.csv")))
    golden = load_golden("spark_cdi_selic.json")
    assert migrated == golden
    assert legacy == golden
    june_cdi = next(row for row in migrated if row["year_month"] == "2024-06" and row["series_id"] == "CDI")
    assert june_cdi["n_obs"] == 3


def test_cvm_scd_matches_golden(spark) -> None:
    snapshot = (
        spark.read.option("header", True)
        .option("delimiter", ";")
        .schema(SNAPSHOT_SCHEMA)
        .csv(str(FIXTURES / "cvm_cad_fi.csv"))
    )
    dim = spark.read.option("header", True).schema(DIM_SCHEMA).csv(str(FIXTURES / "cvm_dim_current.csv"))
    migrated = scd2(snapshot, dim, LOGICAL_DATE)
    legacy = legacy_scd(
        spark.sparkContext.textFile(str(FIXTURES / "cvm_cad_fi.csv")),
        spark.sparkContext.textFile(str(FIXTURES / "cvm_dim_current.csv")),
        LOGICAL_DATE,
    )
    golden = load_golden("spark_cvm_scd.json")
    assert migrated == golden
    assert legacy == golden
    current_alpha = next(
        row for row in migrated if row["cnpj"] == "12345678000190" and row["is_current"] == "Y"
    )
    assert current_alpha["denom_social"] == "FUNDO ALPHA RENDA FIXA"
    assert current_alpha["valid_from"] == LOGICAL_DATE


def test_anbima_curve_normalize_matches_golden(spark) -> None:
    frame = (
        spark.read.option("header", True)
        .option("delimiter", ";")
        .schema(CURVE_SCHEMA)
        .csv(str(FIXTURES / "anbima_ettj.csv"))
    )
    migrated = transform_curves(frame, LOGICAL_DATE)
    legacy = legacy_curves(spark.sparkContext.textFile(str(FIXTURES / "anbima_ettj.csv")), LOGICAL_DATE)
    golden = load_golden("spark_anbima_curves.json")
    assert migrated == golden
    assert legacy == golden
    assert migrated[0]["implied_inflation"] == "0.05250000"


def test_cdi_aggregation_rejects_empty(spark) -> None:
    frame = spark.createDataFrame([], RATE_SCHEMA)
    assert transform_aggregation(frame) == []
