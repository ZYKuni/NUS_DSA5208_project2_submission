"""Real-data v2 acceptance checks on 2023 validation only; no refit/test rescore."""
import json
from pyspark.sql import functions as F, Window
from src.common import load_config, get_spark
from src.task2.run import OUT, FEATURES, PREDS, fingerprints, save_json
from src.predict_safe import predict, KNOTS, validate_inputs


def main():
    frozen = json.loads((OUT/"frozen_manifest.json").read_text())
    assert frozen["hashes"] == fingerprints(), "v1 evidence must remain intact"
    cfg = load_config("configs/project.yaml")
    cfg["spark"].update(master="local[2]", driver_memory="2g", shuffle_partitions=4)
    spark = get_spark(cfg, "v2-acceptance-no-test-rescore")
    try:
        # Locate a real S50 validation case with largest raw upward jump, not
        # an outcome-selected example. No labels or 2024 predictions are read.
        eligible = spark.read.parquet(str(FEATURES/"prepared")).where("split='validation' AND station_id='S50' AND no_recent_data=0").select("station_id", "prediction_time")
        old = spark.read.parquet(str(PREDS/"validation"/"M3_5")).select("station_id", "prediction_time", "threshold_mm", "raw_probability").join(eligible, ["station_id", "prediction_time"]).where(F.col("threshold_mm").isin(*KNOTS))
        w = Window.partitionBy("station_id", "prediction_time").orderBy("threshold_mm")
        chosen = old.withColumn("jump", F.col("raw_probability")-F.lag("raw_probability").over(w)).orderBy(F.desc("jump"), "prediction_time").first()
        outputs = [predict(spark, "S50", chosen.prediction_time, q) for q in [0., .2, .5, 1., 2., 3., 5., 10.]]
        probabilities = [r["probability"] for r in outputs]
        assert all(a >= b for a,b in zip(probabilities, probabilities[1:]))
        assert all(r["curve_probabilities"] == outputs[0]["curve_probabilities"] for r in outputs)
        rejected = []
        for timestamp, q in [("2025-01-01 12:00:00", 1), ("2024-01-01 12:01:00",1), ("2024-01-01 12:00:00",11)]:
            try:
                validate_inputs("S50", timestamp, q)
            except ValueError as exc:
                rejected.append({"time": timestamp, "q": q, "error": str(exc)})
            else:
                raise AssertionError("Unsupported request was accepted")
        save_json(OUT/"safe_v2"/"acceptance.json", {"passed": True, "v1_frozen_hashes_unchanged": True,
            "source_split": "2023 validation", "v2_holdout_performance": "not measured; original v1 scores must not be reused",
            "selection": "largest raw threshold increase for S50 with usable history in validation; no label selection",
            "original_case": chosen.asDict(), "separate_request_outputs": outputs, "rejections": rejected})
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
