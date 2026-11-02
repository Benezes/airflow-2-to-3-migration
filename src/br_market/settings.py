from __future__ import annotations

import os
from pathlib import Path

CVM_CADASTRO_URL = "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"
BCB_SERIES_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados"
ANBIMA_HTTP_ENDPOINT = "/informacoes/est-termo/ettj"
POSTGRES_CONN_ID = "postgres_market"
STAGING_CONN_ID = "postgres_staging"
WAREHOUSE_CONN_ID = "postgres_warehouse"
AWS_CONN_ID = "aws_default"
HTTP_CONN_ID = "anbima_http"
SPARK_CONN_ID = "spark_default"
S3_BUCKET = "br-market-public"
REF_DATE = "2024-06-10"


def fixture_dir() -> Path | None:
    raw = os.environ.get("BR_MARKET_FIXTURE_DIR")
    return Path(raw) if raw else None


def landing_dir() -> Path:
    return Path(os.environ.get("BR_MARKET_LANDING_DIR", "/tmp/br_market/landing"))


def store_dir() -> Path | None:
    raw = os.environ.get("BR_MARKET_STORE_DIR")
    return Path(raw) if raw else None


def s3_local_dir() -> Path | None:
    raw = os.environ.get("BR_MARKET_S3_LOCAL")
    return Path(raw) if raw else None


def spark_output_dir() -> Path:
    return Path(os.environ.get("BR_MARKET_SPARK_OUTPUT", "/tmp/br_market/spark"))
