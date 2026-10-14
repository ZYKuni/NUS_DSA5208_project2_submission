"""Run from repo root: python -m src.task2.run prepare|train|calibrate|test.

Test is gated by a frozen manifest containing model/config/source hashes.
Large features, predictions and models remain local ignored artifacts.
"""
import argparse
import csv
import hashlib
import json
import platform
import subprocess
import time
from decimal import Decimal
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, GBTClassifier
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import IsotonicRegression, IsotonicRegressionModel
from pyspark.ml.functions import vector_to_array

from src.common import get_spark, load_config
from .labels import build_future_30m_base, assign_temporal_split, expand_thresholds
from .features import build_history, add_calendar, CALENDAR, HISTORY
from .evaluation import evaluate

ROOT = Path(__file__).resolve().parents[2]
CONFIG = Path("configs/c_model.json")
OUT = Path("reports/c_results")
MODELS = Path("models/final/c_available_v1")
FEATURES = Path("data/features/c_available_v1")
PREDS = Path("data/predictions/c_available_v1")


def save_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, default=str, allow_nan=False)+"\n")


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8*1024*1024), b""):
            h.update(block)
    return h.hexdigest()


def fingerprints():
    paths = [CONFIG, Path("configs/project.yaml"), Path("src/common.py"), Path("src/clean_data.py"),
             Path("data/manifest.csv"), *Path("src/task2").glob("*.py"), *MODELS.rglob("*"),
             *(FEATURES/"prepared").rglob("*")]
    return {str(p): sha(p) for p in paths if p.is_file() and not p.name.startswith(".")}


def sample_origins(df, cfg):
    bucket = F.pmod(F.xxhash64("station_id", F.col("prediction_time").cast("long"), F.lit(cfg["seed"])),
                    F.lit(cfg["hash_modulus"]))
    limit = F.when(F.col("split") == "train", cfg["train_hash_buckets"]).otherwise(cfg["evaluation_hash_buckets"])
    return df.where(bucket < limit)


def expand(base, thresholds):
    return expand_thresholds(base, thresholds).withColumn("log_threshold", F.log1p("threshold_mm"))


def prediction_table(df, model_id, source="raw"):
    if "calibrated_probability" not in df.columns:
        df = df.withColumn("calibrated_probability", F.lit(None).cast("double"))
    return (df.withColumn("model_id", F.lit(model_id))
        .withColumn("probability_source", F.lit(source)).withColumn("decision_threshold", F.lit(.5))
        .withColumn("prediction", (F.col(source+"_probability") >= .5).cast("int"))
        .withColumn("is_fixture", F.lit(False)).withColumn("inference_mode", F.lit("available_at_proxy"))
        .select("station_id", "prediction_time", "threshold_mm", "future_30m_mm", "label", "label_available",
                "raw_probability", "calibrated_probability", "prediction", "decision_threshold",
                "probability_source", "split", "model_id", "feature_complete", "is_fixture", "inference_mode"))


def export_prediction(df, split, name, source="raw"):
    path = PREDS / split / name
    prediction_table(df, name, source).write.mode("error").parquet(str(path))
    return str(path)


def prepare(spark, cfg):
    expected = list(csv.DictReader(subprocess.check_output(
        ["git", "show", "21ee7ad:data/manifest.csv"], text=True).splitlines()))
    manifest_path = OUT/"download_manifest.csv"
    if not manifest_path.exists():
        manifest_path = Path("data/manifest.csv")
    actual = list(csv.DictReader(manifest_path.open()))
    assert {r["year"]: r["sha256"] for r in actual} == {r["year"]: r["sha256"] for r in expected}
    for row in expected:
        assert sha(Path("data/raw")/f"rainfall_{row['year']}.csv") == row["sha256"]
    clean = spark.read.parquet("data/clean/clean_rainfall")
    by_year = [r.asDict() for r in clean.groupBy("year").count().orderBy("year").collect()]
    assert sum(r["count"] for r in by_year) == 48538627
    assert str(clean.schema["rainfall_mm"].dataType) == "DecimalType(10,3)"
    # Hand-verifiable real wet window, exclusively from training years.
    wet = clean.where("year < 2023 AND rainfall_mm > 0").orderBy("event_time", "station_id").first()
    end = spark.createDataFrame([(wet.event_time,)], "t timestamp").select((F.col("t")+F.expr("INTERVAL 30 MINUTES")).alias("t")).first().t
    small = clean.where((F.col("station_id") == wet.station_id) & F.col("event_time").between(wet.event_time, end))
    records = small.select("event_time", "rainfall_mm", "quality_ok").orderBy("event_time").collect()
    assert len(records) == 7
    check = build_future_30m_base(small).orderBy("prediction_time").first()
    assert check.future_30m_mm == sum((r.rainfall_mm for r in records[1:]), Decimal(0))
    save_json(OUT/"h1_acceptance.json", {"by_year": by_year, "checksums_match_A": True,
        "schema": clean.schema.jsonValue(), "real_window": [r.asDict() for r in records], "computed": check.asDict(),
        "interval_end": "explicit assumption, not independently established by official metadata",
        "availability": cfg["availability"]})
    base_path = FEATURES/"base_full"
    assign_temporal_split(build_future_30m_base(clean)).write.mode("error").partitionBy("split").parquet(str(base_path))
    base = spark.read.parquet(str(base_path))
    save_json(OUT/"label_counts.json", [r.asDict() for r in base.groupBy("split", "label_available", "label_drop_reason").count().collect()])
    valid = base.where("split IS NOT NULL AND label_available")
    # Baseline fitted on all training origins; no test prevalence used.
    train = valid.where("split='train'")
    rates = train.agg(*[F.avg((F.col("future_30m_mm") > F.lit(Decimal(str(q)))).cast("double")).alias(f"p{i}")
                        for i, q in enumerate(cfg["evaluation_thresholds"])]).first()
    baseline = {str(q): rates[f"p{i}"] for i, q in enumerate(cfg["evaluation_thresholds"])}
    save_json(MODELS/"baseline.json", baseline)
    anchors = sample_origins(valid, cfg)
    (add_calendar(build_history(anchors, clean)).write.mode("error").partitionBy("split").parquet(str(FEATURES/"prepared")))
    data = spark.read.parquet(str(FEATURES/"prepared"))
    save_json(OUT/"split_manifest.json", [r.asDict() for r in data.groupBy("split").agg(
        F.count("*").alias("origins"), F.countDistinct("station_id").alias("stations"),
        F.min("prediction_time").alias("start"), F.max("prediction_time").alias("end"),
        F.avg("count_60").alias("mean_known_60m_slots"), F.avg("no_recent_data").alias("no_recent_fraction"),
        F.avg(F.col("feature_complete").cast("double")).alias("complete_fraction")).collect()])
    # Only training targets influence support review. Test target distribution remains unopened.
    training = expand(data.where("split='train'"), cfg["training_thresholds"])
    save_json(OUT/"training_event_counts.json", [r.asDict() for r in training.groupBy("threshold_mm").agg(
        F.count("*").alias("n"), F.sum("label").alias("positive_count"), F.avg("label").alias("positive_rate")).collect()])


def pipeline(family, parameter, cfg):
    columns = CALENDAR + (HISTORY if family != "M1" else [])
    stages = [StringIndexer(inputCol="station_id", outputCol="station_index", handleInvalid="keep"),
              OneHotEncoder(inputCols=["station_index"], outputCols=["station_vector"], handleInvalid="keep"),
              VectorAssembler(inputCols=["station_vector", *columns], outputCol="features")]
    if family == "M3":
        stages.append(GBTClassifier(maxDepth=int(parameter), maxIter=cfg["gbt_max_iter"], seed=cfg["seed"], maxBins=32))
    else:
        stages.append(LogisticRegression(regParam=float(parameter), maxIter=cfg["lr_max_iter"]))
    return Pipeline(stages=stages)


def score(model, data):
    return model.transform(data).withColumn("raw_probability", vector_to_array("probability")[1])


def m0(spark, data):
    baseline = json.loads((MODELS/"baseline.json").read_text())
    lookup = spark.createDataFrame([(float(k), v) for k,v in baseline.items()], "threshold_mm double,raw_probability double")
    return data.join(F.broadcast(lookup), "threshold_mm")


def train(spark, cfg):
    base = spark.read.parquet(str(FEATURES/"prepared"))
    training = expand(base.where("split='train'"), cfg["training_thresholds"]).cache()
    validation = expand(base.where("split='validation'"), cfg["evaluation_thresholds"]).cache()
    training.count(); validation.count()
    baseline = m0(spark, validation).cache()
    save_json(OUT/"validation_M0.json", evaluate(baseline))
    export_prediction(baseline, "validation", "M0")
    baseline.unpersist()
    runs, best = [], {}
    for family in ["M1", "M2", "M3"]:
        params = cfg["gbt_depths"] if family == "M3" else cfg["lr_reg_params"]
        for parameter in params:
            name = f"{family}_{parameter}"
            started = time.time()
            fitted = pipeline(family, parameter, cfg).fit(training)
            fitted.write().save(str(MODELS/name))
            pred = score(fitted, validation).cache()
            metrics = evaluate(pred)
            score_value = sum(r["brier"] for r in metrics["metrics"] if r["threshold_mm"] in cfg["training_thresholds"])/len(cfg["training_thresholds"])
            record = {"name": name, "family": family, "parameter": parameter, "macro_brier": score_value, "seconds": time.time()-started}
            runs.append(record)
            save_json(OUT/f"validation_{name}.json", metrics)
            export_prediction(pred, "validation", name)
            pred.unpersist()
            if family not in best or score_value < best[family]["macro_brier"]:
                best[family] = record
            save_json(OUT/"experiments.json", {"runs": runs, "family_best": best})
            print("EXPERIMENT", record, flush=True)
    main = min(best.values(), key=lambda r: r["macro_brier"])["name"]
    save_json(MODELS/"selection.json", {"main": main, "family_best": best, "criterion": cfg["selection"]})
    training.unpersist(); validation.unpersist()


def calibrate(spark, cfg):
    selection = json.loads((MODELS/"selection.json").read_text())
    calibration = expand(spark.read.parquet(str(FEATURES/"prepared")).where("split='calibration'"), cfg["training_thresholds"]).cache()
    calibration.count()
    for result in selection["family_best"].values():
        name = result["name"]
        model = PipelineModel.load(str(MODELS/name))
        attrs = model.transform(calibration).schema["features"].metadata["ml_attr"]["attrs"]
        names = {a["idx"]: a["name"] for kind in attrs.values() for a in kind}
        estimator = model.stages[-1]
        is_gbt = result["family"] == "M3"
        values = estimator.featureImportances if is_gbt else estimator.coefficients
        save_json(OUT/f"model_diagnostics_{name}.json", {
            "kind": "GBT impurity importance" if is_gbt else "LR original-scale coefficient",
            "warning": "Predictive association, not causal attribution; correlated variables share signal.",
            "features": [{"name": names.get(i, str(i)), "value": float(value)} for i,value in enumerate(values)]})
        raw = score(model, calibration).cache()
        fitted = IsotonicRegression(featuresCol="raw_probability", labelCol="label", predictionCol="calibrated_probability", isotonic=True).fit(raw)
        fitted.write().save(str(MODELS/(name+"_calibrator")))
        pred = fitted.transform(raw).cache()
        save_json(OUT/f"calibration_{name}.json", {"raw": evaluate(pred), "calibrated": evaluate(pred, "calibrated_probability"),
            "warning": "calibration fitted on these rows: in-sample diagnostics, not holdout evidence"})
        export_prediction(pred, "calibration", name, "calibrated")
        pred.unpersist(); raw.unpersist()
    calibration.unpersist()
    save_json(OUT/"frozen_manifest.json", {"frozen_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "config": cfg, "selection": selection, "hashes": fingerprints(), "test_opened": False,
        "python": platform.python_version(), "spark": spark.version,
        "java": spark.sparkContext._jvm.java.lang.System.getProperty("java.version")})


def final_test(spark, cfg):
    frozen = json.loads((OUT/"frozen_manifest.json").read_text())
    assert frozen["hashes"] == fingerprints(), "Model/config/source changed after freeze"
    if (OUT/"test_started.json").exists():
        raise RuntimeError("Test already opened. Preserve results; do not silently repeat or tune.")
    save_json(OUT/"test_started.json", {"opened_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "freeze_sha256": sha(OUT/"frozen_manifest.json")})
    data = expand(spark.read.parquet(str(FEATURES/"prepared")).where("split='test'"), cfg["evaluation_thresholds"]).cache()
    data.count()
    baseline = m0(spark, data).cache()
    save_json(OUT/"test_M0.json", {"raw": evaluate(baseline)})
    export_prediction(baseline, "test", "M0")
    baseline.unpersist()
    for result in frozen["selection"]["family_best"].values():
        name = result["name"]
        model = PipelineModel.load(str(MODELS/name))
        calibrator = IsotonicRegressionModel.load(str(MODELS/(name+"_calibrator")))
        pred = calibrator.transform(score(model, data)).cache()
        save_json(OUT/f"test_{name}.json", {"raw": evaluate(pred), "calibrated": evaluate(pred, "calibrated_probability")})
        path = export_prediction(pred, "test", name, "calibrated")
        # Persist bounded explicit errors for discussion, with natural units/time.
        errors = (prediction_table(pred, name, "calibrated").withColumn("absolute_error", F.abs(F.col("calibrated_probability")-F.col("label")))
                  .orderBy(F.desc("absolute_error")).limit(20))
        save_json(OUT/f"errors_{name}.json", [r.asDict() for r in errors.collect()])
        reload_pred = spark.read.parquet(path)
        assert reload_pred.where("raw_probability IS NULL OR calibrated_probability IS NULL OR isnan(raw_probability) OR isnan(calibrated_probability) OR raw_probability < 0 OR raw_probability > 1 OR calibrated_probability < 0 OR calibrated_probability > 1").count() == 0
        pred.unpersist()
    # Predefined serving rule: calendar-only fallback when no usable history.
    parts = []
    for is_fallback, name in [(False, frozen["selection"]["main"]),
                              (True, frozen["selection"]["family_best"]["M1"]["name"])]:
        subset = data.where((F.col("no_recent_data") == 1) if is_fallback else (F.col("no_recent_data") == 0))
        fitted = PipelineModel.load(str(MODELS/name))
        calibrated = IsotonicRegressionModel.load(str(MODELS/(name+"_calibrator"))).transform(score(fitted, subset))
        parts.append(prediction_table(calibrated, name, "calibrated"))
    serving = parts[0].unionByName(parts[1]).cache()
    save_json(OUT/"test_serving.json", {"raw": evaluate(serving), "calibrated": evaluate(serving, "calibrated_probability")})
    serving.write.mode("error").parquet(str(PREDS/"test"/"serving"))
    serving.unpersist()
    data.unpersist()
    save_json(OUT/"test_complete.json", {"completed_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "freeze_sha256": sha(OUT/"frozen_manifest.json")})


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("stage", choices=["prepare", "train", "calibrate", "test"])
    args = parser.parse_args()
    cfg = json.loads(CONFIG.read_text())
    spark_cfg = load_config("configs/project.yaml")
    spark_cfg["spark"].update(master="local[4]", driver_memory="4g", shuffle_partitions=32)
    spark = get_spark(spark_cfg, "C-"+args.stage)
    started = time.time()
    try:
        {"prepare": prepare, "train": train, "calibrate": calibrate, "test": final_test}[args.stage](spark, cfg)
        save_json(OUT/(args.stage+"_timing.json"), {"seconds": time.time()-started, "spark_config": spark_cfg["spark"], "cpu_count": __import__('os').cpu_count()})
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
