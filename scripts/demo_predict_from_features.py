"""Load the saved models and predict from the sampled feature tables shipped with the submission.

Unlike src.predict_safe, this demo needs no raw or cleaned rainfall data: it reads the
prediction-time features already stored in data/features/c_available_v1/prepared, so it
only works for the sampled (station, time) origins in that table. It applies exactly the
serving rule and v2 monotone curve of src.predict_safe and, for test origins, checks the
result against the saved serving predictions.

Run from the repository root:
    python -m scripts.demo_predict_from_features --list 5
    python -m scripts.demo_predict_from_features S06 '2024-11-07 20:50:00' 1
"""
import argparse
import json
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.regression import IsotonicRegressionModel
from pyspark.sql import SparkSession, functions as F

from src.predict_safe import KNOTS, interpolate, monotone_curve, validate_inputs
from src.task2.run import FEATURES, MODELS, PREDS, score


def spark_session():
    return (SparkSession.builder.master("local[2]").appName("DSA5208-demo-predict")
            .config("spark.sql.session.timeZone", "Asia/Singapore")
            .config("spark.sql.shuffle.partitions", "4").getOrCreate())


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("station_id", nargs="?")
    parser.add_argument("prediction_time", nargs="?", help="Asia/Singapore time on a five-minute boundary")
    parser.add_argument("threshold_mm", nargs="?", type=float, help="threshold q in [0, 10] mm")
    parser.add_argument("--split", default="test", choices=["train", "validation", "calibration", "test"])
    parser.add_argument("--list", type=int, metavar="N", help="print N available (station, time) origins and exit")
    args = parser.parse_args()

    spark = spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    features = spark.read.parquet(str(FEATURES/"prepared")).where(F.col("split") == args.split)
    if args.list:
        # date_format renders in the session time zone (SGT), unlike Python-side datetimes.
        for r in (features.orderBy("prediction_time", "station_id").limit(args.list)
                  .select("station_id", F.date_format("prediction_time", "yyyy-MM-dd HH:mm:ss").alias("t")).collect()):
            print(r.station_id, r.t)
        return
    if args.threshold_mm is None:
        parser.error("give STATION TIME Q, or --list N")

    try:
        t, q = validate_inputs(args.station_id, args.prediction_time, args.threshold_mm)
    except ValueError as exc:
        raise SystemExit(str(exc))
    origin = features.where((F.col("station_id") == args.station_id) &
                            (F.col("prediction_time") == F.lit(t.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))).cache()
    row = origin.first()
    if row is None:
        raise SystemExit(f"{args.station_id} at {t.isoformat()} is not a sampled {args.split} origin; "
                         "use --list N to see available origins, or src.predict_safe with the full cleaned history")

    selection = json.loads((MODELS/"selection.json").read_text())
    fallback = row.no_recent_data == 1
    name = selection["family_best"]["M1"]["name"] if fallback else selection["main"]
    model = PipelineModel.load(str(MODELS/name))
    calibrator = IsotonicRegressionModel.load(str(MODELS/(name+"_calibrator")))
    thresholds = spark.createDataFrame([(x,) for x in sorted(set(KNOTS+(q,)))], "threshold_mm double")
    inputs = origin.crossJoin(F.broadcast(thresholds)).withColumn("log_threshold", F.log1p("threshold_mm"))
    by_q = {r.threshold_mm: r for r in calibrator.transform(score(model, inputs))
            .select("threshold_mm", "raw_probability", "calibrated_probability").collect()}
    curve = monotone_curve([by_q[x].calibrated_probability for x in KNOTS])
    result = {"station_id": args.station_id, "prediction_time": t.isoformat(), "threshold_mm": q,
              "probability": interpolate(curve, q), "model_id": name, "calendar_fallback": fallback,
              "curve_thresholds_mm": list(KNOTS), "curve_probabilities": curve,
              "observed_next_30m_mm": None if row.future_30m_mm is None else float(row.future_30m_mm),
              "observed_exceeds_q": None if row.future_30m_mm is None else float(row.future_30m_mm) > q}

    saved_path = PREDS/args.split/"serving"
    if saved_path.exists():
        saved = {r.threshold_mm: r.calibrated_probability for r in spark.read.parquet(str(saved_path))
                 .where((F.col("station_id") == args.station_id) & (F.col("prediction_time") == row.prediction_time))
                 .select("threshold_mm", "calibrated_probability").collect()}
        common = [x for x in saved if x in by_q]
        if common:
            result["max_abs_diff_vs_saved_serving"] = max(abs(saved[x]-by_q[x].calibrated_probability) for x in common)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
