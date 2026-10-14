"""Coverage arithmetic on a hand-built clean table (test fixture, not real data)."""
import datetime as dt
from decimal import Decimal

from pyspark.sql import functions as F

from data_quality import calendar_days, roll_up, station_day_coverage


def _clean(spark):
    rows = []
    # S1: every slot of 2024-02-29 (leap day) plus 10 slots on 2024-03-01
    start = dt.datetime(2024, 2, 29)
    for i in range(288):
        rows.append(("S1", start + dt.timedelta(minutes=5 * i), Decimal("0.2") if i < 3 else Decimal(0)))
    for i in range(10):
        rows.append(("S1", dt.datetime(2024, 3, 1) + dt.timedelta(minutes=5 * i), Decimal(0)))
    return spark.createDataFrame(rows, "station_id string, timestamp_sgt timestamp, rainfall_mm decimal(10,3)")


def test_coverage(spark):
    clean = _clean(spark)
    stations = clean.groupBy("station_id").agg(
        F.min("timestamp_sgt").alias("first_obs"), F.max("timestamp_sgt").alias("last_obs"))
    days = calendar_days(spark, "2024-01-01", "2025-01-01")
    assert days.count() == 366

    day = station_day_coverage(clean, stations, days)
    leap = day.filter(F.col("date") == dt.date(2024, 2, 29)).first()
    assert leap.n_valid == 288 and leap.coverage == 1.0 and leap.n_wet == 3
    jan = day.filter(F.col("date") == dt.date(2024, 1, 15)).first()
    assert jan.n_valid == 0 and not jan.in_active_range  # before first obs: not "missing"

    month = {r.month: r for r in roll_up(day, ["station_id", "year", "month"]).collect()}
    assert month[2].calendar_days == 29 and month[2].active_days == 1
    assert month[2].coverage_active == 1.0
    assert abs(month[2].coverage_calendar - 1 / 29) < 1e-12
    assert month[3].active_days == 1 and month[3].n_valid == 10
    assert month[1].coverage_active is None

    year = roll_up(day, ["station_id", "year"]).first()
    assert year.expected_slots_calendar == 366 * 288 and year.n_valid == 298
