"""As-of features: exact lag joins, using only versions published by the origin."""
import math
from pyspark.sql import DataFrame, functions as F

CALENDAR = ["hour_sin", "hour_cos", "doy_sin", "doy_cos", "log_threshold"]
HISTORY = ([f"lag_{m}" for m in [5, 10, 15, 30, 60]]
           + [f"missing_{m}" for m in [5, 10, 15, 30, 60]]
           + [f"{name}_{m}" for m in [15, 30, 60] for name in ["sum", "count", "max", "wet"]]
           + ["recent_change", "staleness_minutes", "no_recent_data"])


def add_calendar(df: DataFrame) -> DataFrame:
    hour = F.hour("prediction_time") + F.minute("prediction_time") / 60.
    days = F.dayofyear(F.to_date(F.concat(F.year("prediction_time"), F.lit("-12-31"))))
    phase = (F.dayofyear("prediction_time") - 1) / days
    return (df.withColumn("hour_sin", F.sin(hour * (2 * math.pi / 24)))
            .withColumn("hour_cos", F.cos(hour * (2 * math.pi / 24)))
            .withColumn("doy_sin", F.sin(phase * 2 * math.pi))
            .withColumn("doy_cos", F.cos(phase * 2 * math.pi)))


def build_history(anchors: DataFrame, observations: DataFrame) -> DataFrame:
    """12 slots in (t-60min,t], including current only if published by t.

    Missing/unpublished lags become zero *only in ML input*, paired with explicit
    missing flags. Window sums are sums of known rain, with counts/completeness;
    they do not pretend unobserved slots are dry. Staleness is capped at 65 min
    with no_recent_data=1 when no usable observation exists in this window.
    Coordinates are omitted to avoid injecting metadata from an unpublished row.
    """
    keys = ["station_id", "prediction_time"]
    needed = (anchors.select(*keys).withColumn("_offset", F.explode(F.sequence(F.lit(0), F.lit(11))))
              .withColumn("_lag_time", (F.col("prediction_time").cast("long")-F.col("_offset")*300).cast("timestamp")))
    obs = observations.select(F.col("station_id").alias("_station"),
        F.col("event_time").alias("_event"), F.col("rainfall_mm").cast("double").alias("_rain"),
        "available_at", "available_at_basis", "quality_ok")
    joined = needed.join(obs, (F.col("station_id") == F.col("_station")) & (F.col("_lag_time") == F.col("_event")), "left")
    usable = (F.col("quality_ok") & (F.col("available_at_basis") == "update_timestamps")
              & (F.col("available_at") <= F.col("prediction_time"))
              & (F.col("_event") <= F.col("prediction_time"))
              & F.col("_rain").isNotNull() & ~F.isnan("_rain")
              & (F.col("_rain") >= 0) & (F.col("_rain") < float("inf")))
    joined = joined.withColumn("_known", F.when(usable, F.col("_rain")))
    aggs = []
    for minutes in [5, 10, 15, 30]:
        value = F.max(F.when(F.col("_offset") == minutes // 5, F.col("_known")))
        aggs.extend([F.coalesce(value, F.lit(0.)).alias(f"lag_{minutes}"),
                     value.isNull().cast("double").alias(f"missing_{minutes}")])
    for minutes in [15, 30, 60]:
        value = F.when(F.col("_offset") < minutes // 5, F.col("_known"))
        aggs.extend([F.coalesce(F.sum(value), F.lit(0.)).alias(f"sum_{minutes}"),
                     F.count(value).cast("double").alias(f"count_{minutes}"),
                     F.coalesce(F.max(value), F.lit(0.)).alias(f"max_{minutes}"),
                     F.sum(F.when(value > 0, 1).otherwise(0)).cast("double").alias(f"wet_{minutes}")])
    last = F.min(F.when(F.col("_known").isNotNull(), F.col("_offset")*5))
    aggs.extend([F.coalesce(last, F.lit(65)).cast("double").alias("staleness_minutes"),
                 last.isNull().cast("double").alias("no_recent_data")])
    result = joined.groupBy(*keys).agg(*aggs)
    # lag 60 is the excluded left endpoint of the 60-minute aggregate.
    lag60 = anchors.select(*keys).join(obs,
        (F.col("station_id") == F.col("_station")) &
        (F.col("_event").cast("long") == F.col("prediction_time").cast("long") - 3600), "left")
    value60 = F.when(usable, F.col("_rain"))
    lag60 = lag60.select(*keys, F.coalesce(value60, F.lit(0.)).alias("lag_60"),
                         value60.isNull().cast("double").alias("missing_60"))
    return (anchors.join(result, keys).join(lag60, keys)
            .withColumn("recent_change", 2*F.col("sum_15")-F.col("sum_30"))
            .withColumn("feature_complete", (F.col("count_60") == 12) & (F.col("missing_60") == 0)))
