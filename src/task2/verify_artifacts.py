"""Independent new-session reload/CLI-parity checks, only after final test completes."""
from pyspark.sql import functions as F
from src.common import get_spark, load_config
from .run import OUT, PREDS, fingerprints, save_json
from .predict import predict
import json


def main():
    assert (OUT/"test_complete.json").exists(), "Do not inspect test predictions before final evaluation"
    frozen = json.loads((OUT/"frozen_manifest.json").read_text())
    assert frozen["hashes"] == fingerprints()
    cfg = load_config("configs/project.yaml")
    cfg["spark"].update(master="local[2]", driver_memory="2g", shuffle_partitions=4)
    spark = get_spark(cfg, "C-model-reload-check")
    try:
        row = (spark.read.parquet(str(PREDS/"test"/"serving")).where(F.col("threshold_mm") == 1.)
               .orderBy("station_id", "prediction_time").first())
        result = predict(spark, row.station_id, row.prediction_time, row.threshold_mm)
        assert abs(result["raw_probability"]-row.raw_probability) < 1e-12
        assert abs(result["probability"]-row.calibrated_probability) < 1e-12
        assert result["model_id"] == row.model_id
        unknown = predict(spark, "__unseen_station_smoke_test__", "2024-01-01 12:00:00", .5)
        assert unknown["calendar_fallback"] and not unknown["known_training_station"]
        assert unknown["known_slots_60m"] == 0 and 0 <= unknown["probability"] <= 1
        save_json(OUT/"reload_verification.json", {"new_spark_session": True,
            "batch_inference_absolute_tolerance": 1e-12, "batch_record": row.asDict(),
            "inference_result": result, "unknown_station_fallback": unknown, "passed": True})
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
