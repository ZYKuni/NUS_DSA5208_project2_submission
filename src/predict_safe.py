"""v2 guarded, monotone rainfall interface; frozen v1 models remain unchanged.

Post-hoc interface revision: v1 holdout scores do NOT evaluate this v2 output.
"""
import argparse
from bisect import bisect_right
from datetime import datetime
import json
import math
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.regression import IsotonicRegressionModel
from src.common import get_spark, load_config
from src.task2.features import build_history, add_calendar
from src.task2.run import MODELS, score

SGT = ZoneInfo("Asia/Singapore")
KNOTS = (0., .2, 1., 2., 5., 10.)
VERSION = "c_safe_v2"


def validate_inputs(station_id, prediction_time, threshold_mm):
    if not isinstance(station_id, str) or not station_id.strip():
        raise ValueError("station_id must be non-empty")
    if isinstance(threshold_mm, bool):
        raise ValueError("threshold_mm must be numeric, not boolean")
    try:
        q = float(threshold_mm)
    except (ValueError, TypeError) as exc:
        raise ValueError("threshold_mm must be finite and within [0, 10] mm") from exc
    if not math.isfinite(q) or not 0 <= q <= 10:
        raise ValueError("UNSUPPORTED_THRESHOLD: supported q is [0, 10] mm; q>10 is not extrapolated or clamped")
    try:
        t = datetime.fromisoformat(prediction_time) if isinstance(prediction_time, str) else prediction_time
        if not isinstance(t, datetime):
            raise ValueError()
        if t.tzinfo is None:
            t = t.replace(tzinfo=SGT)
        t = t.astimezone(SGT)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError("prediction_time must be ISO 8601; no timezone means Asia/Singapore (UTC+08:00)") from exc
    if t.second or t.microsecond or t.minute % 5:
        raise ValueError("UNALIGNED_TIME: exact five-minute endpoint required; seconds/microseconds must be zero; no automatic rounding")
    if not datetime(2017, 1, 1, tzinfo=SGT) <= t < datetime(2025, 1, 1, tzinfo=SGT):
        raise ValueError("UNSUPPORTED_TIME: this archive interface supports 2017–2024 SGT only; no silent calendar fallback")
    return t, q


def monotone_curve(values):
    """Equal-weight non-increasing least-squares PAVA on the six FIXED knots.

    Request q is never added to the projection grid, so separate requests use
    the same curve. No labels, fitted tail distribution or epsilon clipping.
    """
    if len(values) != len(KNOTS) or any(not math.isfinite(v) or not 0 <= v <= 1 for v in values):
        raise ValueError("Expected six finite knot probabilities in [0,1]")
    blocks = []
    for v in values:
        blocks.append([float(v), 1])
        while len(blocks) > 1 and blocks[-2][0]/blocks[-2][1] < blocks[-1][0]/blocks[-1][1]:
            right, left = blocks.pop(), blocks.pop()
            blocks.append([left[0]+right[0], left[1]+right[1]])
    return [total/count for total, count in blocks for _ in range(count)]


def interpolate(curve, q):
    if not math.isfinite(q) or not KNOTS[0] <= q <= KNOTS[-1]:
        raise ValueError("No threshold extrapolation is supported")
    i = min(bisect_right(KNOTS, q)-1, len(KNOTS)-2)
    weight = (q-KNOTS[i])/(KNOTS[i+1]-KNOTS[i])
    return max(curve[i+1], min(curve[i], curve[i] + weight*(curve[i+1]-curve[i])))


def checked_history(spark, history_path, station_id, t):
    obs = spark.read.parquet(str(history_path)).where(F.col("station_id") == station_id)
    bounds = obs.agg(F.min(F.col("event_time").cast("long")).alias("first"),
                     F.max(F.col("event_time").cast("long")).alias("last")).first()
    if bounds.first is None:
        raise ValueError(f"UNKNOWN_STATION: {station_id!r} has no observations in this history store")
    stamp = int(t.timestamp())
    if not bounds.first <= stamp <= bounds.last:
        start, end = [datetime.fromtimestamp(x, SGT).isoformat() for x in (bounds.first, bounds.last)]
        raise ValueError(f"OUTSIDE_STATION_COVERAGE: {station_id} history is [{start}, {end}]; no silent calendar fallback")
    return obs.where((F.col("event_time").cast("long") >= stamp-3600) &
                     (F.col("event_time").cast("long") <= stamp))


def predict(spark, station_id, prediction_time, threshold_mm,
            history_path="data/clean/clean_rainfall", model_root=MODELS):
    t, q = validate_inputs(station_id, prediction_time, threshold_mm)
    observations = checked_history(spark, history_path, station_id, t)
    anchors = spark.createDataFrame([(station_id, t)], "station_id string,prediction_time timestamp")
    prepared = add_calendar(build_history(anchors, observations)).cache()
    try:
        quality = prepared.select("no_recent_data", "count_60", "staleness_minutes").first()
        selection = json.loads((Path(model_root)/"selection.json").read_text())
        fallback = quality.no_recent_data == 1
        name = selection["family_best"]["M1"]["name"] if fallback else selection["main"]
        model = PipelineModel.load(str(Path(model_root)/name))
        calibrator = IsotonicRegressionModel.load(str(Path(model_root)/(name+"_calibrator")))
        thresholds = spark.createDataFrame([(x,) for x in sorted(set(KNOTS+(q,)))], "threshold_mm double")
        inputs = prepared.crossJoin(F.broadcast(thresholds)).withColumn("log_threshold", F.log1p("threshold_mm"))
        rows = calibrator.transform(score(model, inputs)).select("threshold_mm", "raw_probability", "calibrated_probability").collect()
        by_q = {r.threshold_mm: r for r in rows}
        original = [by_q[x].calibrated_probability for x in KNOTS]
        curve = monotone_curve(original)
        p = interpolate(curve, q)
        warnings = []
        if fallback:
            warnings.append({"code": "CALENDAR_FALLBACK", "message": "No usable published observation in (t-60min,t]; using calendar-only model, not a history-based nowcast."})
        known = station_id in model.stages[0].labels
        if not known:
            warnings.append({"code": "UNSEEN_TRAINING_STATION", "message": "Station exists in history but was not fitted in this model; unknown-category encoding used."})
        if p in (0., 1.):
            warnings.append({"code": "BOUNDARY_ESTIMATE", "message": "Estimated probability is exactly 0 or 1; this is not certainty about the event. No arbitrary epsilon has been added."})
        return {"interface_version": VERSION, "model_id": name, "station_id": station_id,
                "prediction_time": t.isoformat(), "threshold_mm": q, "probability": p,
                "raw_probability": by_q[q].raw_probability,
                "unadjusted_calibrated_probability": by_q[q].calibrated_probability,
                "calendar_fallback": fallback, "fallback_reason": "no_usable_published_history_60m" if fallback else None,
                "known_training_station": known, "known_slots_60m": quality.count_60,
                "staleness_minutes_capped": quality.staleness_minutes,
                "threshold_extrapolation": False, "supported_threshold_mm": [0., 10.],
                "monotonicity_guaranteed": True, "monotonicity_scope": "fixed station/time/history/model, q in [0,10]",
                "adjustment": "fixed-six-knot equal-weight decreasing PAVA, linear interpolation in mm",
                "curve_thresholds_mm": list(KNOTS), "curve_probabilities": curve,
                "boundary_probability": p in (0., 1.), "warnings": warnings,
                "inference_mode": "available_at_proxy", "evaluation_status": "v2 post-hoc interface; v1 holdout metrics do not apply"}
    finally:
        prepared.unpersist()


def parser():
    p = argparse.ArgumentParser(description="v2 guarded monotone rainfall probability. Naive time means Asia/Singapore (UTC+08:00). No automatic time rounding; no threshold extrapolation.")
    p.add_argument("station_id", help="Station must exist in the history store; time must lie within its observed span")
    p.add_argument("prediction_time", help="ISO 8601; no timezone = Asia/Singapore (UTC+08:00); exact five-minute boundary, seconds=0; 2017–2024 only. Not rounded.")
    p.add_argument("threshold_mm", type=float, help="Rainfall threshold in [0,10] mm; q>10 is rejected, never clamped/extrapolated")
    p.add_argument("--history", default="data/clean/clean_rainfall")
    p.add_argument("--model-root", default=str(MODELS))
    return p


def main():
    cli = parser()
    args = cli.parse_args()
    try:
        validate_inputs(args.station_id, args.prediction_time, args.threshold_mm)
    except ValueError as exc:
        cli.error(str(exc))
    cfg = load_config("configs/project.yaml")
    cfg["spark"].update(master="local[2]", driver_memory="2g", shuffle_partitions=4)
    spark = get_spark(cfg, VERSION)
    try:
        result = predict(spark, args.station_id, args.prediction_time, args.threshold_mm, args.history, args.model_root)
        for warning in result["warnings"]:
            print(f"WARNING {warning['code']}: {warning['message']}", file=sys.stderr)
        print(json.dumps(result, allow_nan=False))
    except ValueError as exc:
        cli.error(str(exc))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
