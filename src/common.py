"""Shared helpers: configuration, SparkSession, raw schema."""
from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]

RAW_COLUMNS = [
    "date", "timestamp", "update_timestamp", "station_id", "station_name",
    "station_device_id", "location_longitude", "location_latitude",
    "reading_update_timestamp", "reading_value", "reading_type", "reading_unit",
]


def load_config(path: str | os.PathLike) -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def repo_path(cfg: dict, key: str) -> Path:
    """Resolve a `paths.<key>` entry against the repository root."""
    p = Path(cfg["paths"][key])
    return p if p.is_absolute() else REPO_ROOT / p


def base_arg_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--config", default=str(REPO_ROOT / "configs" / "project.yaml"))
    return parser


def setup_logging(cfg: dict, name: str) -> logging.Logger:
    logs_dir = repo_path(cfg, "logs_dir")
    logs_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(logs_dir / f"{name}.log")],
    )
    return logging.getLogger(name)


def raw_schema():
    """Explicit schema: every raw column is read as a string so nothing is
    silently coerced; `_corrupt_record` captures malformed lines."""
    from pyspark.sql.types import StringType, StructField, StructType

    fields = [StructField(c, StringType(), True) for c in RAW_COLUMNS]
    fields.append(StructField("_corrupt_record", StringType(), True))
    return StructType(fields)


def get_spark(cfg: dict, app_name: str):
    from pyspark.sql import SparkSession

    # Python converts collected/created naive datetimes with the process time
    # zone; pin it (and the JVM's) to SGT so driver-side values match Spark SQL.
    tz = cfg["project"]["timezone"]
    os.environ["TZ"] = tz
    time.tzset()
    sc = cfg["spark"]
    builder = (
        SparkSession.builder.appName(app_name)
        .master(sc["master"])
        .config("spark.driver.memory", sc["driver_memory"])
        .config("spark.sql.shuffle.partitions", str(sc["shuffle_partitions"]))
        .config("spark.sql.warehouse.dir", str(REPO_ROOT / "spark-warehouse"))
        .config("spark.sql.session.timeZone", tz)
        .config("spark.driver.extraJavaOptions", f"-Duser.timezone={tz}")
        .config("spark.executor.extraJavaOptions", f"-Duser.timezone={tz}")
        .config("spark.executorEnv.TZ", tz)
    )
    for k, v in (sc.get("extra") or {}).items():
        builder = builder.config(k, str(v))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_small_csv(df, path: Path) -> None:
    """Write a small aggregate DataFrame as one local CSV (via pandas).
    Only for bounded summary tables, never for full-size data."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.toPandas().to_csv(path, index=False)
