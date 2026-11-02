from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

from tests.conftest import ROOT

AIR3 = ["AIR301", "AIR302", "AIR311", "AIR312"]
AFTER_PATHS = [ROOT / "after", ROOT / "spark" / "after", ROOT / "src" / "br_market"]
BEFORE_PATHS = [ROOT / "before", ROOT / "spark" / "before"]


def _ruff_bin() -> str:
    found = shutil.which("ruff")
    if found:
        return found
    for candidate in (
        Path(sys.prefix) / "bin" / "ruff",
        Path(sys.executable).parent / "ruff",
    ):
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError("ruff is not installed")


def _ruff(paths, extra: list[str] | None = None) -> subprocess.CompletedProcess:
    cmd = [_ruff_bin(), "check", *[str(path) for path in paths], "--select", ",".join(AIR3)]
    if extra:
        cmd.extend(extra)
    return subprocess.run(cmd, cwd=ROOT, check=False, capture_output=True, text=True)


def test_after_is_clean_for_air3_rules() -> None:
    result = _ruff(AFTER_PATHS)
    assert result.returncode == 0, result.stdout + result.stderr


def test_before_triggers_air3_findings() -> None:
    result = _ruff(BEFORE_PATHS, extra=["--output-format", "json"])
    assert result.returncode != 0
    findings = json.loads(result.stdout)
    assert findings, result.stderr
    codes = {item["code"] for item in findings}
    assert "AIR301" in codes
    messages = " ".join(item.get("message") or "" for item in findings)
    assert "schedule_interval" in messages
    assert "execution_date" in messages
    assert "airflow.contrib" in messages or "contrib" in messages
