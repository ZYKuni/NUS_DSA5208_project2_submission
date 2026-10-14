from datetime import datetime, timedelta
import json
import random
from types import SimpleNamespace

import pytest
from pyspark.sql import functions as F
from pyspark.ml.functions import array_to_vector
import src.predict_safe as safe


@pytest.mark.parametrize("q", [-1, 10.001, 20, 1e20, float("nan"), float("inf"), True])
def test_reject_unsupported_threshold(q):
    with pytest.raises(ValueError):
        safe.validate_inputs("S50", "2024-01-01 12:00:00", q)


@pytest.mark.parametrize("t", ["2025-01-01 00:00:00", "2016-12-31 23:55:00"])
def test_reject_archive_extrapolation(t):
    with pytest.raises(ValueError, match="UNSUPPORTED_TIME"):
        safe.validate_inputs("S50", t, 1)


@pytest.mark.parametrize("t", ["2024-01-01 12:01:00", "2024-01-01 12:05:01", "2024-01-01 12:05:00.000001"])
def test_time_is_not_rounded(t):
    with pytest.raises(ValueError, match="no automatic rounding"):
        safe.validate_inputs("S50", t, 1)


def test_timezone_and_help():
    naive, _ = safe.validate_inputs("S50", "2024-01-01 12:05:00", 0)
    utc, _ = safe.validate_inputs("S50", "2024-01-01T04:05:00+00:00", 10)
    assert naive == utc and naive.isoformat().endswith("+08:00")
    help_text = safe.parser().format_help()
    for text in ["Asia/Singapore", "UTC+08:00", "Not rounded", "q>10"]:
        assert text in help_text


def test_pava_repairs_point_22_increase_and_does_not_invent_tail():
    curve = safe.monotone_curve([.8, .2, .42, .1, .05, 0.])
    assert curve == pytest.approx([.8, .31, .31, .1, .05, 0.])
    assert safe.interpolate(curve, 10) == 0
    with pytest.raises(ValueError):
        safe.interpolate(curve, 10.01)
    monotone = [.9, .8, .4, .3, .2, .1]
    assert safe.monotone_curve(monotone) == monotone


def test_separate_queries_share_one_monotone_curve():
    rng = random.Random(42)
    for _ in range(100):
        curve = safe.monotone_curve([rng.random() for _ in safe.KNOTS])
        qs = sorted(set([0., .2, 1., 2., 5., 10.] + [rng.uniform(0,10) for _ in range(100)]))
        ps = [safe.interpolate(curve, q) for q in qs]
        assert all(0 <= p <= 1 for p in ps)
        assert all(a >= b for a, b in zip(ps, ps[1:]))


def test_spark_guard_fallback_and_boundary_warning(spark, tmp_path, monkeypatch):
    t = datetime(2024, 1, 1, 12)
    # Events exist before/after t, but none of the final versions is published by t.
    rows = [("S50", t+timedelta(minutes=m), .2, t+timedelta(days=1), "update_timestamps", True) for m in [-60,0,60]]
    obs = spark.createDataFrame(rows, "station_id string,event_time timestamp,rainfall_mm double,available_at timestamp,available_at_basis string,quality_ok boolean")
    history = tmp_path/"history"
    obs.write.parquet(str(history))
    with pytest.raises(ValueError, match="OUTSIDE_STATION_COVERAGE"):
        safe.checked_history(spark, history, "S50", t.replace(tzinfo=safe.SGT)+timedelta(days=1))
    with pytest.raises(ValueError, match="UNKNOWN_STATION"):
        safe.checked_history(spark, history, "unknown", t.replace(tzinfo=safe.SGT))
    (tmp_path/"selection.json").write_text(json.dumps({"main": "M3_5", "family_best": {"M1": {"name": "M1_0.01"}}}))

    class FakeModel:
        stages = [SimpleNamespace(labels=["S50"])]

        def transform(self, df):
            p = F.when(F.col("threshold_mm") == 0, .8).when(F.col("threshold_mm") == .2, .2).when(F.col("threshold_mm") == 1, .42).when(F.col("threshold_mm") == 2, .1).when(F.col("threshold_mm") == 5, .05).otherwise(0.)
            return df.withColumn("probability", array_to_vector(F.array(1-p, p)))

    class FakeCalibrator:
        def transform(self, df):
            return df.withColumn("calibrated_probability", F.col("raw_probability"))

    monkeypatch.setattr(safe.PipelineModel, "load", lambda _: FakeModel())
    monkeypatch.setattr(safe.IsotonicRegressionModel, "load", lambda _: FakeCalibrator())
    result = safe.predict(spark, "S50", t, 10, history, tmp_path)
    assert result["calendar_fallback"] and result["model_id"] == "M1_0.01"
    assert result["fallback_reason"] == "no_usable_published_history_60m"
    assert result["boundary_probability"] and result["probability"] == 0.
    assert {r["code"] for r in result["warnings"]} == {"CALENDAR_FALLBACK", "BOUNDARY_ESTIMATE"}
    assert result["curve_probabilities"] == pytest.approx([.8,.31,.31,.1,.05,0])
