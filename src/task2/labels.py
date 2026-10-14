"""Spark label construction on A's unique, aligned canonical observations."""
import math
from decimal import Decimal, ROUND_FLOOR, localcontext
from numbers import Real

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import BooleanType, NumericType, StringType, TimestampType


def build_future_30m_base(
    observations_df: DataFrame,
    station_col: str = "station_id",
    time_col: str = "event_time",
    rainfall_col: str = "rainfall_mm",
    quality_col: str = "quality_ok",
) -> DataFrame:
    """Return one anchor per input station/time; invalid targets remain auditable.

    Requires unique keys and exact five-minute endpoints. No gap filling or
    duplicate resolution is performed here. Validation triggers Spark actions.
    available_at is deliberately irrelevant to outcome truth, but future values
    and validity flags must never be passed to a feature assembler.
    """
    required = {station_col: StringType, time_col: TimestampType,
                rainfall_col: NumericType, quality_col: BooleanType}
    for name, kind in required.items():
        if name not in observations_df.columns or not isinstance(
            observations_df.schema[name].dataType, kind
        ):
            raise ValueError(f"{name} must have Spark type {kind.__name__}")
    df = observations_df.select(
        F.col(station_col).alias("station_id"),
        F.col(time_col).alias("prediction_time"),
        F.col(rainfall_col).cast("decimal(20,3)").alias("_rain"),
        F.col(quality_col).alias("_quality"),
    ).withColumn("_seconds", F.col("prediction_time").cast("double"))
    bad_key = (F.col("station_id").isNull() | (F.trim("station_id") == "")
               | F.col("prediction_time").isNull()
               | (F.pmod("_seconds", F.lit(300)) != 0))
    if df.where(bad_key).limit(1).count():
        raise ValueError("Keys must be non-null and times exact five-minute endpoints")
    if df.groupBy("station_id", "prediction_time").count().where("count > 1").limit(1).count():
        raise ValueError("Duplicate station/time keys: resolve in A's cleaning layer")
    valid = (F.col("_quality") & F.col("_rain").isNotNull()
             & (F.col("_rain") >= 0))
    df = (df.withColumn("_valid_rain", F.when(valid, F.col("_rain")))
          .withColumn("_seconds", F.col("_seconds").cast("long")))
    window = Window.partitionBy("station_id").orderBy("_seconds").rangeBetween(300, 1800)
    df = (df.withColumn("future_slots", F.count(F.lit(1)).over(window))
          .withColumn("future_count", F.count("_valid_rain").over(window)))
    return df.select(
        "station_id", "prediction_time", "future_slots", "future_count",
        (F.col("future_count") == 6).alias("label_available"),
        F.when(F.col("future_slots") < 6, F.lit("missing_future_slot"))
         .when(F.col("future_count") < 6, F.lit("invalid_reading"))
         .otherwise(F.lit(None).cast("string")).alias("label_drop_reason"),
        F.when(F.col("future_count") == 6, F.sum("_valid_rain").over(window))
         .alias("future_30m_mm"),
    )


def assign_temporal_split(base_df: DataFrame) -> DataFrame:
    """Keep boundary anchors for auditing, with null split if target crosses it."""
    if base_df.sparkSession.conf.get("spark.sql.session.timeZone") != "Asia/Singapore":
        raise ValueError("Set spark.sql.session.timeZone=Asia/Singapore")
    ranges = [
        ("train", "2017-01-01", "2023-01-01"),
        ("validation", "2023-01-01", "2023-10-01"),
        ("calibration", "2023-10-01", "2024-01-01"),
        ("test", "2024-01-01", "2025-01-01"),
    ]
    split = F.lit(None).cast("string")
    anchor = F.col("prediction_time")
    target_end = anchor + F.expr("INTERVAL 30 MINUTES")
    for name, start, end in ranges:
        split = F.when((anchor >= F.to_timestamp(F.lit(start)))
                       & (target_end < F.to_timestamp(F.lit(end))), name).otherwise(split)
    return base_df.withColumn("split", split)


def expand_thresholds(base_df: DataFrame, thresholds) -> DataFrame:
    """Expand an already split table; preserve unavailable labels as null.

    Boundaries outside a split are retained for audit with null label. Filter
    split IS NOT NULL AND label IS NOT NULL before training/evaluation.
    """
    if "split" not in base_df.columns:
        raise ValueError("Assign temporal splits before expanding thresholds")
    values = list(thresholds)
    if not values or any(isinstance(q, bool) or not isinstance(q, (Real, Decimal))
                         or not math.isfinite(float(q)) or q < 0 for q in values):
        raise ValueError("Thresholds must be finite non-negative real numbers")
    decimal_values = [Decimal(str(q)) for q in values]
    if len(set(decimal_values)) != len(values):
        raise ValueError("Duplicate thresholds are not allowed")
    # Outcomes are exact multiples of .001 mm from A's canonical Decimal(10,3).
    # S > q iff S_in_micrometres > floor(q*1000). Preserve even thresholds
    # immediately below .600; never round q up or use an epsilon.
    rows = []
    for q in decimal_values:
        with localcontext() as ctx:
            ctx.prec = max(50, len(q.as_tuple().digits) + abs(q.as_tuple().exponent) + 5)
            units = (q * 1000).to_integral_value(rounding=ROUND_FLOOR)
        # Decimal(20,3) input bounds ensure any 30m sum is below this ceiling.
        units = min(units, Decimal("999999999999999999999999999999"))
        rows.append(F.struct(F.lit(float(q)).alias("value"),
                             F.lit(units).cast("decimal(38,0)").alias("units")))
    expanded = (base_df.withColumn("_q", F.explode(F.array(*rows)))
                .withColumn("threshold_mm", F.col("_q.value")))
    return expanded.withColumn(
        "label", F.when(F.col("label_available") & F.col("split").isNotNull(),
                        ((F.col("future_30m_mm").cast("decimal(30,3)") * 1000)
                         > F.col("_q.units")).cast("double"))
    ).drop("_q")
