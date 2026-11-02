from decimal import Decimal
import csv
import io
import json
from datetime import datetime

from legacy_etl.money import money, rate8, to_decimal

ACTIVE = "EM FUNCIONAMENTO NORMAL"
JCP_IR = Decimal("0.15")


def normalize_cnpj(value):
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) != 14:
        raise ValueError("invalid cnpj: %s" % value)
    return digits


def _parse_date(value):
    text = value.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass
    raise ValueError("invalid date: %s" % value)


def parse_cvm_cadastro(csv_text, ref_date):
    reader = csv.DictReader(io.StringIO(csv_text), delimiter=";")
    kept = []
    skipped = 0
    read = 0
    for raw in reader:
        read += 1
        sit = (raw.get("SIT") or "").strip()
        if sit != ACTIVE:
            skipped += 1
            continue
        kept.append(
            {
                "cnpj": normalize_cnpj(raw["CNPJ_FUNDO"]),
                "denom_social": (raw.get("DENOM_SOCIAL") or "").strip(),
                "classe": (raw.get("CLASSE") or "").strip(),
                "sit": sit,
                "dt_reg": _parse_date(raw["DT_REG"]),
                "vl_patrim_liq": money(raw.get("VL_PATRIM_LIQ") or "0"),
                "ref_date": ref_date,
            }
        )
    kept.sort(key=lambda row: row["cnpj"])
    return {"rows": kept, "counters": {"read": read, "kept": len(kept), "skipped": skipped}}


def parse_b3_indicators(payload, ref_date):
    data = json.loads(payload) if not isinstance(payload, dict) else payload
    rows = []
    items = data["indices"]
    for item in items:
        close = to_decimal(item["close"])
        prev_close = to_decimal(item["prev_close"])
        if prev_close == 0:
            change = Decimal("0")
        else:
            change = (close / prev_close) - 1
        rows.append(
            {
                "code": item["code"],
                "ref_date": data.get("ref_date") or ref_date,
                "close": money(close),
                "prev_close": money(prev_close),
                "daily_return": rate8(change),
            }
        )
    rows.sort(key=lambda row: row["code"])
    return {"rows": rows, "counters": {"read": len(items), "kept": len(rows), "skipped": 0}}


def parse_anbima_ettj(csv_text, ref_date):
    reader = csv.DictReader(io.StringIO(csv_text), delimiter=";")
    rows = []
    read = 0
    for raw in reader:
        read += 1
        curve_date = _parse_date(raw["data"])
        if curve_date != ref_date:
            continue
        rows.append(
            {
                "ref_date": curve_date,
                "curva": raw["curva"].strip().upper(),
                "vertice_dias": int(raw["vertice_dias"]),
                "taxa": rate8(raw["taxa"]),
            }
        )
    rows.sort(key=lambda row: (row["curva"], row["vertice_dias"]))
    return {"rows": rows, "counters": {"read": read, "kept": len(rows), "skipped": read - len(rows)}}


def parse_bcb_series(payload, series_id, ref_date):
    data = json.loads(payload) if not isinstance(payload, list) else payload
    rows = []
    skipped = 0
    for item in data:
        parsed = _parse_date(item["data"])
        if parsed != ref_date:
            skipped += 1
            continue
        rows.append(
            {
                "ref_date": parsed,
                "series_id": series_id,
                "rate_pct": money(item["valor"]),
            }
        )
    rows.sort(key=lambda row: (row["series_id"], row["ref_date"]))
    return {"rows": rows, "counters": {"read": len(data), "kept": len(rows), "skipped": skipped}}


def sync_rate_rows(staging_rows, source_system):
    synced = []
    for row in staging_rows:
        synced.append(
            {
                "ref_date": row["ref_date"],
                "series_id": row["series_id"],
                "rate_pct": money(row["rate_pct"]),
                "source_system": source_system,
            }
        )
    synced.sort(key=lambda row: (row["series_id"], row["ref_date"]))
    return synced


def parse_proventos(csv_text, ref_date):
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = []
    skipped = 0
    read = 0
    for raw in reader:
        read += 1
        ex_date = _parse_date(raw["data_ex"])
        if ex_date != ref_date:
            skipped += 1
            continue
        rows.append(
            {
                "ticker": raw["ticker"].strip().upper(),
                "tipo": raw["tipo"].strip().upper(),
                "data_ex": ex_date,
                "data_pagamento": _parse_date(raw["data_pagamento"]),
                "valor_por_cota": money(raw["valor_por_cota"]),
            }
        )
    rows.sort(key=lambda row: row["ticker"])
    return {"rows": rows, "counters": {"read": read, "kept": len(rows), "skipped": skipped}}


def parse_holdings(csv_text):
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = []
    for raw in reader:
        rows.append({"ticker": raw["ticker"].strip().upper(), "cotas": to_decimal(raw["cotas"])})
    return rows


def apply_proventos(proventos, holdings, ref_date):
    by_ticker = {}
    for row in holdings:
        by_ticker[row["ticker"]] = row["cotas"]
    applied = []
    skipped = 0
    for event in proventos:
        cotas = by_ticker.get(event["ticker"])
        if cotas is None:
            skipped += 1
            continue
        gross = to_decimal(event["valor_por_cota"]) * cotas
        if event["tipo"] == "JCP":
            ir = gross * JCP_IR
        else:
            ir = Decimal("0")
        applied.append(
            {
                "ticker": event["ticker"],
                "tipo": event["tipo"],
                "ref_date": ref_date,
                "data_ex": event["data_ex"],
                "data_pagamento": event["data_pagamento"],
                "cotas": money(cotas),
                "valor_por_cota": money(event["valor_por_cota"]),
                "gross_amount": money(gross),
                "ir_amount": money(ir),
                "net_amount": money(gross - ir),
            }
        )
    applied.sort(key=lambda row: row["ticker"])
    return {"rows": applied, "counters": {"read": len(proventos), "kept": len(applied), "skipped": skipped}}


def implied_inflation(pre_rate, ipca_rate):
    return rate8(to_decimal(pre_rate) - to_decimal(ipca_rate))
