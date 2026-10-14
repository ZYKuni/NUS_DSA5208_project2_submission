"""Hand-built raw rows covering every cleaning rule (test fixture, not real data)."""
import csv
import datetime as dt

import pytest
from pyspark.sql import functions as F

from clean_data import assign_fates, cleaning_log, parse_raw, read_raw, to_clean
from common import RAW_COLUMNS

TYPE = "TB1 Rainfall 5 Minute Total F"


def row(ts, value, station="S1", upd=None, unit="mm", name="Alpha Road", date=None):
    upd = upd or ts
    return {
        "date": date or ts[:10], "timestamp": ts, "update_timestamp": upd,
        "station_id": station, "station_name": name, "station_device_id": station,
        "location_longitude": "103.8", "location_latitude": "1.3",
        "reading_update_timestamp": upd, "reading_value": value,
        "reading_type": TYPE, "reading_unit": unit,
    }


# (case id, row, expected fate)
CASES = [
    ("zero_kept", row("2017-01-01T00:05:00+08:00", "0.0"), "kept"),
    ("zero_exact_dup", row("2017-01-01T00:05:00+08:00", "0.0"), "exact_duplicate"),
    ("snap_299", row("2017-01-01T00:09:59+08:00", "0.2"), "kept"),
    ("unaligned", row("2017-01-01T00:12:26+08:00", "0.2"), "unaligned_timestamp"),
    ("conflict_old", row("2017-01-01T00:15:00+08:00", "0.4", upd="2017-01-01T00:20:00+08:00"),
     "conflict_superseded"),
    ("conflict_new", row("2017-01-01T00:15:00+08:00", "0.6", upd="2017-01-01T00:25:00+08:00"),
     "kept"),
    ("tie_a", row("2017-01-01T00:20:00+08:00", "1.0", upd="2017-01-01T00:25:00+08:00"),
     "conflict_unresolved"),
    ("tie_b", row("2017-01-01T00:20:00+08:00", "1.2", upd="2017-01-01T00:25:00+08:00"),
     "conflict_unresolved"),
    ("same_old", row("2017-01-01T00:25:00+08:00", "0.2", upd="2017-01-01T00:30:00+08:00"),
     "same_value_merged"),
    ("same_new", row("2017-01-01T00:25:00+08:00", "0.20", upd="2017-01-01T00:31:00+08:00",
                     name="Alpha Rd"), "kept"),
    ("negative", row("2017-01-01T00:30:00+08:00", "-0.2"), "value_negative"),
    ("non_numeric", row("2017-01-01T00:35:00+08:00", "abc"), "value_non_numeric"),
    ("missing", row("2017-01-01T00:40:00+08:00", ""), "value_missing"),
    ("bad_unit", row("2017-01-01T00:45:00+08:00", "0.2", unit="inch"), "type_unit_unexpected"),
    ("before_window", row("2016-12-31T23:55:00+08:00", "0.0"), "out_of_window"),
    ("end_exclusive", row("2025-01-01T00:00:00+08:00", "0.0"), "out_of_window"),
    ("leap_day", row("2024-02-29T12:00:00+08:00", "3.4"), "kept"),
    ("bad_ts", row("not-a-time", "0.0", upd="not-a-time", date="2017-01-01"), "ts_unparseable"),
    ("no_station", row("2017-01-01T00:50:00+08:00", "0.0", station=""), "station_missing"),
    ("precision", row("2017-01-01T00:55:00+08:00", "0.12351"), "kept"),
    ("other_station", row("2017-01-01T00:05:00+08:00", "0.0", station="S2"), "kept"),
    ("year_boundary", row("2017-12-31T23:55:00+08:00", "0.2"), "kept"),
]


@pytest.fixture(scope="module")
def fated(spark, cfg, tmp_path_factory):
    d = tmp_path_factory.mktemp("raw")
    files = {2017: [], 2024: []}
    for case_id, r, _ in CASES:
        files[2024 if case_id in {"leap_day", "end_exclusive"} else 2017].append(r)
    paths = []
    for year, rows in files.items():
        p = d / f"rainfall_{year}.csv"
        with open(p, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=RAW_COLUMNS, quoting=csv.QUOTE_MINIMAL)
            w.writeheader()
            w.writerows(rows)
        paths.append(str(p))
    parsed = parse_raw(read_raw(spark, paths), cfg)
    return assign_fates(parsed).cache()


def _fate_of(fated, ts, value, station="S1"):
    rows = (fated.filter((F.col("timestamp") == ts) & (F.col("reading_value") == value)
                         & (F.col("station_id") == station)).select("fate").collect())
    return sorted(r.fate for r in rows)


def test_every_case_gets_expected_fate(fated):
    got = sorted(r.fate for r in fated.select("fate").collect())
    assert got == sorted(f for _, _, f in CASES)
    assert _fate_of(fated, "2017-01-01T00:05:00+08:00", "0.0") == ["exact_duplicate", "kept"]
    assert _fate_of(fated, "2017-01-01T00:15:00+08:00", "0.6") == ["kept"]
    assert _fate_of(fated, "2017-01-01T00:15:00+08:00", "0.4") == ["conflict_superseded"]
    assert _fate_of(fated, "2017-01-01T00:25:00+08:00", "0.20") == ["kept"]
    assert _fate_of(fated, "2017-01-01T00:12:26+08:00", "0.2") == ["unaligned_timestamp"]


def test_log_reconciles(fated):
    log = cleaning_log(fated).collect()
    assert sum(r.raw_rows for r in log) == len(CASES)
    by_year = {r.source_year: r for r in log}
    assert by_year[2024].kept == 1 and by_year[2024].out_of_window == 1


def test_clean_table(fated):
    clean = to_clean(fated)
    rows = {(r.station_id, r.timestamp_sgt): r for r in clean.collect()}
    assert len(rows) == clean.count()  # key unique
    # +08:00 is parsed as SGT wall time, not shifted again by 8 hours
    t = dt.datetime(2017, 1, 1, 0, 5)
    assert rows[("S1", t)].rainfall_mm == 0  # zero kept as a real observation
    snapped = rows[("S1", dt.datetime(2017, 1, 1, 0, 10))]
    assert snapped.align_shift_s == 1 and snapped.raw_timestamp.endswith("00:09:59+08:00")
    resolved = rows[("S1", dt.datetime(2017, 1, 1, 0, 15))]
    assert float(resolved.rainfall_mm) == 0.6
    assert resolved.quality_flag == "conflict_resolved_latest" and resolved.n_raw_versions == 2
    assert resolved.available_at == dt.datetime(2017, 1, 1, 0, 25)
    assert ("S1", dt.datetime(2017, 1, 1, 0, 20)) not in rows  # unresolved -> quarantine
    assert ("S1", dt.datetime(2017, 1, 1, 0, 30)) not in rows  # negative not turned into 0
    assert rows[("S1", dt.datetime(2024, 2, 29, 12, 0))].year == 2024
    assert rows[("S1", dt.datetime(2017, 12, 31, 23, 55))].month == 12
    rounded = rows[("S1", dt.datetime(2017, 1, 1, 0, 55))]
    assert float(rounded.rainfall_mm) == 0.124 and rounded.value_rounded  # half-up, flagged
    assert not rows[("S1", t)].value_rounded


def test_excess_precision_quarantine_mode(spark, cfg, tmp_path):
    import copy

    p = tmp_path / "rainfall_2017.csv"
    with open(p, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=RAW_COLUMNS)
        w.writeheader()
        w.writerow(row("2017-01-01T00:55:00+08:00", "0.12351"))
    strict = copy.deepcopy(cfg)
    strict["cleaning"]["excess_precision"] = "quarantine"
    got = parse_raw(read_raw(spark, [str(p)]), strict).select("reject_reason").first()
    assert got.reject_reason == "value_precision"


def test_hour_is_sgt(spark, fated):
    clean = to_clean(fated)
    hours = {r.h for r in clean.select(F.hour("timestamp_sgt").alias("h")).collect()}
    assert 0 in hours and 12 in hours and 23 in hours


def test_clean_output_satisfies_task2_contract(fated):
    """A's clean table feeds C's label builder directly (docs/C/task2_contract.md)."""
    from src.task2.labels import build_future_30m_base

    clean = to_clean(fated)
    types = dict(clean.dtypes)
    assert types["event_time"] == "timestamp" and types["quality_ok"] == "boolean"
    assert clean.where("event_time != timestamp_sgt OR NOT quality_ok").count() == 0
    base = build_future_30m_base(clean)  # validates types, 5-min endpoints, unique keys
    assert base.count() == clean.count()
