from datetime import datetime, timedelta
from src.task2.features import build_history, add_calendar


def test_publication_cutoff_and_exact_lags(spark):
    t = datetime(2022, 1, 1, 12)
    anchors = spark.createDataFrame([("S", t)], "station_id string,prediction_time timestamp")
    rows = [
        ("S", t, 100., t+timedelta(minutes=5), "update_timestamps", True),
        ("S", t-timedelta(minutes=5), 9., t+timedelta(seconds=1), "update_timestamps", True),
        ("S", t-timedelta(minutes=10), 2., t, "update_timestamps", True),
        ("S", t-timedelta(minutes=15), 7., t, "slot_only", True),
        ("S", t-timedelta(minutes=60), 3., t, "update_timestamps", True),
        ("S", t+timedelta(minutes=5), 1000., t, "update_timestamps", True),
    ]
    df = spark.createDataFrame(rows, "station_id string,event_time timestamp,rainfall_mm double,available_at timestamp,available_at_basis string,quality_ok boolean")
    r = build_history(anchors, df).first()
    assert r.lag_5 == 0 and r.missing_5 == 1
    assert r.lag_10 == 2 and r.missing_10 == 0
    assert r.lag_15 == 0 and r.missing_15 == 1
    assert r.sum_60 == 2 and r.count_60 == 1
    assert r.lag_60 == 3 and r.missing_60 == 0
    assert r.staleness_minutes == 10 and not r.feature_complete


def test_no_history_and_leap_calendar(spark):
    t = datetime(2024, 12, 31)
    anchors = spark.createDataFrame([("S", t)], "station_id string,prediction_time timestamp")
    obs = spark.createDataFrame([], "station_id string,event_time timestamp,rainfall_mm double,available_at timestamp,available_at_basis string,quality_ok boolean")
    r = add_calendar(build_history(anchors, obs)).first()
    assert r.count_60 == 0 and r.no_recent_data == 1 and r.staleness_minutes == 65
    assert -0.02 < r.doy_sin < 0
