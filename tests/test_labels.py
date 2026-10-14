from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from src.task2.labels import build_future_30m_base, assign_temporal_split, expand_thresholds

SCHEMA = "station_id string, event_time timestamp, rainfall_mm double, quality_ok boolean"


def sequence(start="2022-06-01T12:00:00"):
    t = datetime.fromisoformat(start).replace(tzinfo=ZoneInfo("Asia/Singapore"))
    # Current 99 mm must be excluded; next six sum to exactly 2 mm.
    return [("S1", t + timedelta(minutes=5*i), v, True)
            for i, v in enumerate([99., .25, .25, .5, 0., 1., 0., 50.])]


def first(spark, rows):
    return build_future_30m_base(spark.createDataFrame(rows, SCHEMA)).orderBy("prediction_time").first()


def test_normal_strict_threshold_and_shared_target(spark):
    base = assign_temporal_split(build_future_30m_base(spark.createDataFrame(sequence(), SCHEMA)))
    rows = (expand_thresholds(base, [0., 1., 2., 3.]).orderBy("prediction_time", "threshold_mm")
            .limit(4).collect())
    assert [r.label for r in rows] == [1., 1., 0., 0.]
    assert all(r.future_30m_mm == 2. and r.future_count == 6 and r.split == "train" for r in rows)


def test_missing_slot_not_next_six_rows(spark):
    rows = sequence()
    del rows[2]  # Six later rows still exist, but +10 is absent and +35 is outside target.
    r = first(spark, rows)
    assert r.future_slots == 5 and r.future_count == 5
    assert r.future_30m_mm is None and not r.label_available
    assert r.label_drop_reason == "missing_future_slot"


@pytest.mark.parametrize("value,quality", [(None, True), (float("nan"), True),
    (float("inf"), True), (-1., True), (1., False), (1., None)])
def test_invalid_future_not_zero(spark, value, quality):
    rows = sequence()
    rows[2] = (rows[2][0], rows[2][1], value, quality)
    r = first(spark, rows)
    assert r.future_slots == 6 and r.future_count == 5
    assert r.future_30m_mm is None and r.label_drop_reason == "invalid_reading"


def test_duplicate_rejected(spark):
    rows = sequence()
    with pytest.raises(ValueError, match="Duplicate"):
        first(spark, rows + [rows[2]])


def test_misaligned_rejected(spark):
    rows = sequence()
    rows[2] = ("S1", rows[2][1] - timedelta(seconds=1), .25, True)
    with pytest.raises(ValueError, match="five-minute"):
        first(spark, rows)


@pytest.mark.parametrize("start", ["2022-06-01T23:50:00", "2022-06-30T23:50:00", "2022-12-31T23:50:00"])
def test_calendar_boundaries(spark, start):
    assert first(spark, sequence(start)).future_30m_mm == 2.


def test_station_isolation_and_tail(spark):
    rows = sequence()
    other = [("S2", t, 100., q) for _, t, _, q in rows]
    result = build_future_30m_base(spark.createDataFrame(rows + other, SCHEMA))
    assert result.where("station_id='S1'").orderBy("prediction_time").first().future_30m_mm == 2.
    assert result.orderBy("prediction_time", ascending=False).first().future_count == 0


def test_split_boundaries(spark):
    dates = ["2022-12-31T23:25:00", "2022-12-31T23:30:00", "2023-01-01T00:00:00",
             "2023-09-30T23:30:00", "2023-10-01T00:00:00", "2024-01-01T00:00:00",
             "2024-12-31T23:30:00"]
    frame = spark.createDataFrame([(datetime.fromisoformat(t).replace(tzinfo=ZoneInfo("Asia/Singapore")),)
                                  for t in dates], "prediction_time timestamp")
    assert [r.split for r in assign_temporal_split(frame).orderBy("prediction_time").collect()] == [
        "train", None, "validation", None, "calibration", "test", None]


@pytest.mark.parametrize("thresholds", [[], [-1], [float("nan")], [float("inf")], [True], ["2"], [1, 1.]])
def test_threshold_validation(spark, thresholds):
    df = spark.createDataFrame([("train",)], "split string")
    with pytest.raises(ValueError, match="Threshold|threshold"):
        expand_thresholds(df, thresholds)


def test_split_required(spark):
    with pytest.raises(ValueError, match="splits"):
        expand_thresholds(spark.range(1), [1])


def test_unavailable_and_cross_split_labels_stay_null(spark):
    frame = spark.createDataFrame(
        [("train", False, None), (None, True, 2.), ("train", True, 2.)],
        "split string, label_available boolean, future_30m_mm double")
    result = expand_thresholds(frame, [0., 2.]).collect()
    assert len(result) == 6
    assert sum(r.label is None for r in result) == 4
    assert sorted(r.label for r in result if r.label is not None) == [0., 1.]
