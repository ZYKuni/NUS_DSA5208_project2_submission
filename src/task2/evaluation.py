"""Distributed probability metrics; collect only small aggregate tables."""
from pyspark.sql import Window, functions as F
from pyspark.ml.evaluation import BinaryClassificationEvaluator


def evaluate(df, probability="raw_probability"):
    p = F.col(probability)
    positive = F.col("label") == 1
    alert = p >= .5
    clipped = F.greatest(F.lit(1e-15), F.least(F.lit(1-1e-15), p))
    metrics = df.groupBy("threshold_mm").agg(
        F.count("*").alias("n"), F.sum("label").alias("positive_count"),
        F.avg("label").alias("positive_rate"),
        F.avg((p-F.col("label"))**2).alias("brier"),
        F.avg(-F.col("label")*F.log(clipped)-(1-F.col("label"))*F.log(1-clipped)).alias("log_loss"),
        F.sum((alert & positive).cast("long")).alias("tp"),
        F.sum((alert & ~positive).cast("long")).alias("fp"),
        F.sum((~alert & ~positive).cast("long")).alias("tn"),
        F.sum((~alert & positive).cast("long")).alias("fn"))
    rows = []
    for row in metrics.orderBy("threshold_mm").collect():
        item = row.asDict()
        subset = df.where(F.col("threshold_mm") == row.threshold_mm)
        item.update(roc_auc=None, pr_auc=None)
        if 0 < row.positive_count < row.n:
            ev = BinaryClassificationEvaluator(rawPredictionCol=probability, labelCol="label", numBins=0)
            item["roc_auc"] = ev.setMetricName("areaUnderROC").evaluate(subset)
            item["pr_auc"] = ev.setMetricName("areaUnderPR").evaluate(subset)
        item["precision"] = row.tp/(row.tp+row.fp) if row.tp+row.fp else None
        item["recall"] = row.tp/(row.tp+row.fn) if row.tp+row.fn else None
        item["f1"] = 2*row.tp/(2*row.tp+row.fp+row.fn) if 2*row.tp+row.fp+row.fn else None
        rows.append(item)
    reliability = (df.withColumn("bin", F.least(F.lit(9), F.floor(p*10)))
                   .groupBy("threshold_mm", "bin").agg(F.count("*").alias("n"),
                       F.avg(p).alias("mean_probability"), F.avg("label").alias("event_rate")))
    groups = {}
    for key in ["station_id", "month"]:
        grouped = df.withColumn("month", F.month("prediction_time")).groupBy("threshold_mm", key).agg(
            F.count("*").alias("n"), F.avg("label").alias("positive_rate"),
            F.avg((p-F.col("label"))**2).alias("brier"))
        groups[key] = [r.asDict() for r in grouped.collect()]
    w = Window.partitionBy("station_id", "prediction_time").orderBy("threshold_mm")
    diffs = df.withColumn("_increase", p-F.lag(p).over(w))
    monotonic = diffs.agg(F.count("_increase").alias("pairs"),
        F.sum((F.col("_increase") > 1e-12).cast("long")).alias("violations"),
        F.greatest(F.lit(0.), F.max("_increase")).alias("max_increase")).first().asDict()
    return {"metrics": rows, "reliability": [r.asDict() for r in reliability.collect()],
            "monotonicity": monotonic, "by_group": groups}
