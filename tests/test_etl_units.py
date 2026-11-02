from __future__ import annotations

import pytest

from br_market.http import build_session
from br_market.transforms import normalize_cnpj, parse_cvm_cadastro, parse_proventos


def test_normalize_cnpj_strips_punctuation() -> None:
    assert normalize_cnpj("12.345.678/0001-90") == "12345678000190"


def test_normalize_cnpj_rejects_short_value() -> None:
    with pytest.raises(ValueError):
        normalize_cnpj("123")


def test_cvm_skips_cancelled_funds() -> None:
    raw = (
        "CNPJ_FUNDO;DENOM_SOCIAL;SIT;CLASSE;DT_REG;VL_PATRIM_LIQ\n"
        "12.345.678/0001-90;ALPHA;CANCELADA;Acoes;2010-01-15;1\n"
    )
    parsed = parse_cvm_cadastro(raw, "2024-06-10")
    assert parsed["counters"]["kept"] == 0
    assert parsed["counters"]["skipped"] == 1


def test_proventos_filters_other_dates() -> None:
    raw = "ticker,tipo,data_ex,data_pagamento,valor_por_cota\nPETR4,DIVIDENDO,2024-01-01,2024-01-10,1.00\n"
    parsed = parse_proventos(raw, "2024-06-10")
    assert parsed["rows"] == []
    assert parsed["counters"]["skipped"] == 1


def test_http_session_mounts_retry_adapter() -> None:
    session = build_session()
    assert "https://" in session.adapters
    assert session.adapters["https://"].max_retries.total == 3
