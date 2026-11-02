from __future__ import annotations

from br_market.transforms import (
    apply_proventos,
    parse_anbima_ettj,
    parse_b3_indicators,
    parse_bcb_series,
    parse_cvm_cadastro,
    parse_holdings,
    parse_proventos,
    sync_rate_rows,
)
from legacy_etl.transforms import (
    apply_proventos as legacy_apply_proventos,
    parse_anbima_ettj as legacy_parse_anbima_ettj,
    parse_b3_indicators as legacy_parse_b3_indicators,
    parse_bcb_series as legacy_parse_bcb_series,
    parse_cvm_cadastro as legacy_parse_cvm_cadastro,
    parse_holdings as legacy_parse_holdings,
    parse_proventos as legacy_parse_proventos,
    sync_rate_rows as legacy_sync_rate_rows,
)
from tests.conftest import FIXTURES, LOGICAL_DATE
from tests.helpers import load_golden


def _read(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_cvm_parser_matches_golden_and_legacy() -> None:
    raw = _read("cvm_cad_fi.csv")
    migrated = parse_cvm_cadastro(raw, LOGICAL_DATE)
    legacy = legacy_parse_cvm_cadastro(raw, LOGICAL_DATE)
    golden = load_golden("cvm_cadastro.json")
    assert migrated == golden
    assert legacy == golden


def test_b3_parser_matches_golden_and_legacy() -> None:
    raw = _read("b3_indicators.json")
    migrated = parse_b3_indicators(raw, LOGICAL_DATE)
    legacy = legacy_parse_b3_indicators(raw, LOGICAL_DATE)
    golden = load_golden("b3_indicators.json")
    assert migrated == golden
    assert legacy == golden


def test_anbima_parser_matches_golden_and_legacy() -> None:
    raw = _read("anbima_ettj.csv")
    migrated = parse_anbima_ettj(raw, LOGICAL_DATE)
    legacy = legacy_parse_anbima_ettj(raw, LOGICAL_DATE)
    golden = load_golden("anbima_ettj.json")
    assert migrated == golden
    assert legacy == golden


def test_bcb_and_db_sync_match_golden_and_legacy() -> None:
    cdi = parse_bcb_series(_read("bcb_cdi.json"), "CDI", LOGICAL_DATE)
    selic = parse_bcb_series(_read("bcb_selic.json"), "SELIC", LOGICAL_DATE)
    legacy = legacy_parse_bcb_series(_read("bcb_cdi.json"), "CDI", LOGICAL_DATE)
    assert cdi == legacy
    staging = cdi["rows"] + selic["rows"]
    migrated_sync = sync_rate_rows(staging, "bcb_sgs")
    legacy_sync = legacy_sync_rate_rows(staging, "bcb_sgs")
    golden = load_golden("bcb_rates_mart.json")
    assert migrated_sync == golden
    assert legacy_sync == golden


def test_proventos_application_matches_golden_and_legacy() -> None:
    events = parse_proventos(_read("proventos.csv"), LOGICAL_DATE)
    holdings = parse_holdings(_read("holdings.csv"))
    migrated = apply_proventos(events["rows"], holdings, LOGICAL_DATE)
    legacy = legacy_apply_proventos(
        legacy_parse_proventos(_read("proventos.csv"), LOGICAL_DATE)["rows"],
        legacy_parse_holdings(_read("holdings.csv")),
        LOGICAL_DATE,
    )
    golden = load_golden("proventos_applied.json")
    assert migrated == golden
    assert legacy == golden
