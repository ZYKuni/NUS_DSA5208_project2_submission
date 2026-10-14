import json
from pyspark.sql import functions as F
from pyspark.ml.regression import IsotonicRegression
from src.task2.run import pipeline, score, prediction_table
from src.task2.features import CALENDAR, HISTORY
from src.task2.evaluation import evaluate


def test_candidate_models_calibration_and_evaluation(spark):
    cfg = json.load(open("configs/c_model.json"))
    cfg.update(lr_max_iter=2, gbt_max_iter=2)
    data = (spark.range(40).withColumn("station_id", F.when(F.col("id")%2 == 0, "S1").otherwise("S2"))
            .withColumn("label", (F.col("id")%3 == 0).cast("double")))
    for column in CALENDAR + HISTORY:
        data = data.withColumn(column, (F.col("id")%5).cast("double"))
    data = (data.withColumn("prediction_time", (F.lit(1609459200)+F.col("id")*300).cast("timestamp"))
            .withColumn("threshold_mm", F.lit(1.)).withColumn("future_30m_mm", F.col("label")*2)
            .withColumn("label_available", F.lit(True)).withColumn("feature_complete", F.lit(True))
            .withColumn("split", F.lit("validation")))
    for family in ["M1", "M2", "M3"]:
        model = pipeline(family, 2 if family == "M3" else .1, cfg).fit(data)
        raw = score(model, data)
        calibrator = IsotonicRegression(featuresCol="raw_probability", labelCol="label",
                     predictionCol="calibrated_probability").fit(raw)
        pred = prediction_table(calibrator.transform(raw), family, "calibrated").cache()
        result = evaluate(pred)
        assert result["metrics"][0]["n"] == 40
        assert result["metrics"][0]["brier"] >= 0
        assert pred.where("calibrated_probability < 0 OR calibrated_probability > 1").count() == 0
        pred.unpersist()
