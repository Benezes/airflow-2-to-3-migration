from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
FIXTURES = ROOT / "tests" / "fixtures"
AIRFLOW_HOME = ROOT / ".airflow_test_home"
DAGS_FOLDER = AIRFLOW_HOME / "dags"

for path in (str(SRC), str(ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

_pythonpath_parts = [str(SRC), str(ROOT)]
_existing_pythonpath = os.environ.get("PYTHONPATH", "")
if _existing_pythonpath:
    _pythonpath_parts.append(_existing_pythonpath)
os.environ["PYTHONPATH"] = os.pathsep.join(_pythonpath_parts)

os.environ["AIRFLOW_HOME"] = str(AIRFLOW_HOME)
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(DAGS_FOLDER)
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AIRFLOW__CORE__EXECUTOR"] = "LocalExecutor"
os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] = "90"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{AIRFLOW_HOME}/airflow.db"
os.environ["AIRFLOW__API_AUTH__JWT_SECRET"] = "unit-test-jwt-secret-key-32chars"
os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS"] = "True"
os.environ["AIRFLOW__LOGGING__LOGGING_LEVEL"] = "ERROR"
os.environ["AIRFLOW__LOGGING__FAB_LOGGING_LEVEL"] = "ERROR"
os.environ["BR_MARKET_FIXTURE_DIR"] = str(FIXTURES)
os.environ["BR_MARKET_STORE_DIR"] = str(AIRFLOW_HOME / "store")
os.environ["BR_MARKET_S3_LOCAL"] = str(AIRFLOW_HOME / "s3")
os.environ["BR_MARKET_LANDING_DIR"] = str(AIRFLOW_HOME / "landing")
os.environ["BR_MARKET_SPARK_OUTPUT"] = str(AIRFLOW_HOME / "spark_out")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

LOGICAL_DATE = "2024-06-10"
CONN_FILE = FIXTURES / "connections.yaml"


def _link_dags() -> None:
    DAGS_FOLDER.mkdir(parents=True, exist_ok=True)
    sources = list((ROOT / "after" / "dags").glob("*.py")) + list((ROOT / "spark" / "after" / "dags").glob("*.py"))
    wanted = {path.name for path in sources}
    for existing in DAGS_FOLDER.glob("*.py"):
        if existing.name not in wanted:
            existing.unlink()
    for source in sources:
        target = DAGS_FOLDER / source.name
        if target.is_symlink() or target.exists():
            target.unlink()
        target.symlink_to(source)


@pytest.fixture(scope="session", autouse=True)
def airflow_home() -> Path:
    if AIRFLOW_HOME.exists():
        shutil.rmtree(AIRFLOW_HOME)
    AIRFLOW_HOME.mkdir(parents=True)
    (AIRFLOW_HOME / "store").mkdir()
    (AIRFLOW_HOME / "s3").mkdir()
    (AIRFLOW_HOME / "landing").mkdir()
    (AIRFLOW_HOME / "spark_out").mkdir()
    _link_dags()
    landing = AIRFLOW_HOME / "landing" / "anbima" / LOGICAL_DATE
    landing.mkdir(parents=True)
    shutil.copy(FIXTURES / "anbima_ettj.csv", landing / "ettj.csv")
    from airflow.utils.db import initdb

    initdb()
    yield AIRFLOW_HOME


@pytest.fixture
def reset_store(airflow_home: Path) -> Path:
    store = airflow_home / "store"
    if store.exists():
        shutil.rmtree(store)
    store.mkdir()
    return store


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    pythonpath = os.environ["PYTHONPATH"]
    session = (
        SparkSession.builder.master("local[1]")
        .appName("portfolio-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.executorEnv.PYTHONPATH", pythonpath)
        .config("spark.driverEnv.PYTHONPATH", pythonpath)
        .getOrCreate()
    )
    yield session
    session.stop()
