import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
# A's modules are imported as `common`, `clean_data`; C's as `src.task2.labels`.
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from common import load_config  # noqa: E402


@pytest.fixture(scope="session")
def cfg():
    c = load_config(ROOT / "configs" / "project.yaml")
    c["spark"]["master"] = "local[2]"
    c["spark"]["driver_memory"] = "1g"
    c["spark"]["shuffle_partitions"] = 2
    c["spark"]["extra"] = {**(c["spark"].get("extra") or {}), "spark.ui.enabled": "false"}
    return c


@pytest.fixture(scope="session")
def spark(cfg):
    """One shared session for A's and C's tests; get_spark pins Asia/Singapore."""
    from common import get_spark

    s = get_spark(cfg, "dsa5208-tests")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
