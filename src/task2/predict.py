"""Three-input rainfall inference with automatic as-of history and calendar fallback."""
import argparse
import json
import math
from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.regression import IsotonicRegressionModel
from src.common import get_spark, load_config
from .features import build_history, add_calendar
from .run import MODELS, score


def predict(spark, station_id, prediction_time, threshold_mm,
            history_path="data/clean/clean_rainfall", model_root=MODELS):
    from pathlib import Path
    if not isinstance(station_id, str) or not station_id.strip():
        raise ValueError("station_id must be non-empty")
    if isinstance(threshold_mm, bool):
        raise ValueError("threshold_mm must be numeric, not boolean")
    q = float(threshold_mm)
    if not math.isfinite(q) or q < 0:
        raise ValueError("threshold_mm must be finite and non-negative")
    t = datetime.fromisoformat(prediction_time) if isinstance(prediction_time, str) else prediction_time
    if t.tzinfo is None:
        t = t.replace(tzinfo=ZoneInfo("Asia/Singapore"))
    t = t.astimezone(ZoneInfo("Asia/Singapore"))
    if t.second or t.microsecond or t.minute % 5:
        raise ValueError("prediction_time must be an exact five-minute endpoint")
    anchors = spark.createDataFrame([(station_id, t)], "station_id string,prediction_time timestamp")
    observations = spark.read.parquet(str(history_path)).where(F.col("station_id") == station_id)
    observations = observations.where((F.col("event_time").cast("long") >= int(t.timestamp())-3600) &
                                      (F.col("event_time").cast("long") <= int(t.timestamp())))
    prepared = (add_calendar(build_history(anchors, observations))
                .withColumn("threshold_mm", F.lit(q)).withColumn("log_threshold", F.log1p("threshold_mm"))).cache()
    quality = prepared.select("no_recent_data", "count_60", "staleness_minutes").first()
    model_root = Path(model_root)
    selection = json.loads((model_root/"selection.json").read_text())
    name = selection["main"]
    fallback = quality.no_recent_data == 1
    if fallback:
        name = selection["family_best"]["M1"]["name"]
    model = PipelineModel.load(str(model_root/name))
    calibrator = IsotonicRegressionModel.load(str(model_root/(name+"_calibrator")))
    result = calibrator.transform(score(model, prepared)).select("raw_probability", "calibrated_probability").first()
    known_station = station_id in model.stages[0].labels
    prepared.unpersist()
    return {"station_id": station_id, "prediction_time": t.isoformat(), "threshold_mm": q,
            "probability": result.calibrated_probability, "raw_probability": result.raw_probability,
            "model_id": name, "calendar_fallback": fallback, "known_training_station": known_station,
            "threshold_extrapolation": q > 10., "known_slots_60m": quality.count_60,
            "staleness_minutes_capped": quality.staleness_minutes,
            "inference_mode": "available_at_proxy", "monotonicity_guaranteed": False}


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("station_id")
    parser.add_argument("prediction_time")
    parser.add_argument("threshold_mm", type=float)
    parser.add_argument("--history", default="data/clean/clean_rainfall")
    parser.add_argument("--model-root", default=str(MODELS))
    args = parser.parse_args()
    cfg = load_config("configs/project.yaml")
    cfg["spark"].update(master="local[2]", driver_memory="2g", shuffle_partitions=4)
    spark = get_spark(cfg, "C-inference")
    try:
        print(json.dumps(predict(spark, args.station_id, args.prediction_time, args.threshold_mm, args.history, args.model_root)))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
