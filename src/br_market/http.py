from __future__ import annotations

from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from br_market.settings import fixture_dir


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "br-market-airflow/1.0"})
    return session


def read_text(url: str, fixture_name: str | None = None, timeout: int = 30) -> str:
    fixtures = fixture_dir()
    if fixtures is not None and fixture_name:
        return Path(fixtures, fixture_name).read_text(encoding="utf-8")
    response = build_session().get(url, timeout=timeout)
    response.raise_for_status()
    return response.text
