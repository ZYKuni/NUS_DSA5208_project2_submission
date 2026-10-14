"""P1-02 / P1-04 audits.

    python src/data_quality.py --config configs/project.yaml --stage raw
        Before cleaning. Exact row counts, field profile, reading types/units and
        the timestamp phase (seconds past each 5-minute boundary) per year and
        station, under the alignment rule currently in configs/project.yaml.

    python src/data_quality.py --config configs/project.yaml --stage clean
        After clean_data.py. Station x day/month/year coverage, gap lengths and
        update-latency distributions.

All outputs are small CSVs in reports/tables/ (plus Parquet copies of the
coverage tables in data/clean/quality/).
"""
from __future__ import annotations

import sys

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from clean_data import parse_raw, read_raw
from common import (RAW_COLUMNS, base_arg_parser, get_spark, load_config, repo_path,
                    setup_logging, write_small_csv)

SLOTS_PER_DAY = 288


# ---------------------------------------------------------------- raw stage
def raw_inventory(p: DataFrame) -> DataFrame:
    return (
        p.groupBy("source_year", "source_file")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.col("_corrupt_record").isNotNull().cast("int")).alias("corrupt_rows"),
            F.countDistinct("station_id").alias("n_stations"),
            F.min("ts_obs").alias("min_timestamp"),
            F.max("ts_obs").alias("max_timestamp"),
            F.sum(F.col("ts_obs").isNull().cast("int")).alias("ts_unparseable"),
            F.sum(F.col("date_col_mismatch").cast("int")).alias("date_col_mismatch"),
            F.sum((F.col("slot_offset_s") == 0).cast("int")).alias("on_boundary"),
            F.count(F.when(F.col("align_shift_s") != 0, 1)).alias("snapped_by_rule"),
            F.count(F.when(F.col("reject_reason") == "unaligned_timestamp", 1))
            .alias("unaligned_by_rule"),
            F.count(F.when(F.col("reject_reason") == "out_of_window", 1)).alias("out_of_window"),
            F.sum((F.col("reject_reason").isNull()).cast("int")).alias("valid_rows_before_dedup"),
        )
        .orderBy("source_year")
    )


def field_profile(p: DataFrame) -> DataFrame:
    aggs = [F.count(F.lit(1)).alias("rows")]
    aggs += [F.sum((F.col(c).isNull() | (F.trim(F.col(c)) == "")).cast("int"))
             .alias(f"null_{c}") for c in RAW_COLUMNS]
    aggs += [
        F.sum((F.col("value_dec").isNull() & F.col("value_str").isNotNull()
               & (F.col("value_str") != "")).cast("int")).alias("value_non_numeric"),
        F.sum((F.col("value_dec") < 0).cast("int")).alias("value_negative"),
        F.sum((F.col("value_dec") == 0).cast("int")).alias("value_zero"),
        F.sum((F.col("value_dec") > 0).cast("int")).alias("value_positive"),
        F.sum(F.col("value_rounded").cast("int")).alias("value_gt3_decimals"),
        F.max("value_dec").alias("value_max"),
        F.sum(F.col("latitude").isNull().cast("int")).alias("lat_unparseable"),
        F.sum(F.col("longitude").isNull().cast("int")).alias("lon_unparseable"),
        F.sum((F.col("station_id") != F.col("station_device_id")).cast("int"))
        .alias("station_id_ne_device_id"),
    ]
    return p.groupBy("source_year").agg(*aggs).orderBy("source_year")


def time_phase_by_year(p: DataFrame) -> DataFrame:
    w = Window.partitionBy("source_year")
    return (
        p.filter(F.col("slot_offset_s").isNotNull())
        .groupBy("source_year", "slot_offset_s").count()
        .withColumn("share", F.col("count") / F.sum("count").over(w))
        .orderBy("source_year", F.col("count").desc())
    )


def time_phase_by_station_year(p: DataFrame) -> DataFrame:
    return (
        p.filter(F.col("slot_offset_s").isNotNull())
        .groupBy("source_year", "station_id")
        .agg(F.count(F.lit(1)).alias("rows"),
             F.avg((F.col("slot_offset_s") == 0).cast("double")).alias("share_on_boundary"),
             F.avg((F.col("slot_offset_s") == 299).cast("double")).alias("share_offset_299"),
             F.countDistinct("slot_offset_s").alias("n_distinct_offsets"),
             F.mode("slot_offset_s").alias("most_common_offset"))
        .orderBy("source_year", "station_id")
    )


def seconds_field_by_year(p: DataFrame) -> DataFrame:
    """Distribution of the clock seconds (0-59) of raw timestamps."""
    return (p.filter(F.col("ts_obs").isNotNull())
            .groupBy("source_year", F.second("ts_obs").alias("second")).count()
            .orderBy("source_year", "second"))


def diurnal_jan_apr(p: DataFrame) -> DataFrame:
    """Share of wet 5-min readings by raw clock hour, Jan 1-Apr 24 of each year.
    Checks whether early-2017 timestamps (the ':59' period) are shifted, e.g. by
    8 h, relative to later years: the afternoon convective peak should line up."""
    in_window = (F.month("ts_obs") < 4) | ((F.month("ts_obs") == 4) & (F.dayofmonth("ts_obs") < 25))
    return (p.filter(F.col("ts_obs").isNotNull() & in_window & F.col("value_dec").isNotNull())
            .groupBy("source_year", F.hour("ts_obs").alias("hour"))
            .agg(F.count(F.lit(1)).alias("n"),
                 F.avg((F.col("value_dec") > 0).cast("double")).alias("wet_share"),
                 F.avg("value_dec").alias("mean_mm"))
            .orderBy("source_year", "hour"))


def run_raw(spark, cfg, log) -> None:
    tables = repo_path(cfg, "tables_dir")
    years = sorted(int(y) for y in cfg["source"]["datasets"])
    paths = [str(repo_path(cfg, "raw_dir") / f"rainfall_{y}.csv") for y in years]
    keep = RAW_COLUMNS + ["_corrupt_record", "source_file", "source_year", "ts_obs",
                          "slot_offset_s", "align_shift_s", "value_str", "value_dec", "value_rounded",
                          "latitude", "longitude", "date_col_mismatch", "reject_reason"]
    p = parse_raw(read_raw(spark, paths), cfg).select(*keep).persist(StorageLevel.DISK_ONLY)

    write_small_csv(raw_inventory(p), tables / "raw_inventory.csv")
    write_small_csv(field_profile(p), tables / "field_profile.csv")
    write_small_csv(p.groupBy("source_year", "reading_type", "reading_unit").count()
                    .orderBy("source_year"), tables / "reading_type_unit.csv")
    write_small_csv(time_phase_by_year(p), tables / "time_phase_by_year.csv")
    write_small_csv(time_phase_by_station_year(p), tables / "time_phase_by_station_year.csv")
    write_small_csv(seconds_field_by_year(p), tables / "time_seconds_by_year.csv")
    write_small_csv(diurnal_jan_apr(p), tables / "diurnal_jan_apr_by_year.csv")
    write_small_csv(p.groupBy("source_year", "reject_reason").count()
                    .orderBy("source_year", "reject_reason"), tables / "raw_reject_preview.csv")
    for r in raw_inventory(p).collect():
        log.info("raw %s", r.asDict())
    p.unpersist()


# -------------------------------------------------------------- clean stage
def calendar_days(spark, start: str, end: str) -> DataFrame:
    return spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), date_sub(to_date('{end}'), 1),"
        f" interval 1 day)) AS date")


def station_day_coverage(clean: DataFrame, stations: DataFrame, days: DataFrame) -> DataFrame:
    obs = (clean.groupBy("station_id", F.to_date("timestamp_sgt").alias("date"))
           .agg(F.count(F.lit(1)).alias("n_valid"),
                F.sum((F.col("rainfall_mm") > 0).cast("int")).alias("n_wet")))
    grid = stations.select("station_id", F.to_date("first_obs").alias("first_date"),
                           F.to_date("last_obs").alias("last_date")).crossJoin(days)
    return (
        grid.join(obs, ["station_id", "date"], "left").na.fill(0, ["n_valid", "n_wet"])
        .withColumn("expected_slots", F.lit(SLOTS_PER_DAY))
        .withColumn("coverage", F.col("n_valid") / SLOTS_PER_DAY)
        # first/last observation is only a proxy for commissioning/decommissioning
        .withColumn("in_active_range",
                    (F.col("date") >= F.col("first_date")) & (F.col("date") <= F.col("last_date")))
        .withColumn("year", F.year("date")).withColumn("month", F.month("date"))
        .drop("first_date", "last_date")
    )


def roll_up(day_cov: DataFrame, keys: list[str]) -> DataFrame:
    active = F.col("in_active_range")
    out = (
        day_cov.groupBy(*keys).agg(
            F.count(F.lit(1)).alias("calendar_days"),
            F.sum(active.cast("int")).alias("active_days"),
            F.sum("n_valid").alias("n_valid"),
            F.sum("n_wet").alias("n_wet"),
            F.sum((F.col("n_valid") == SLOTS_PER_DAY).cast("int")).alias("full_days"),
            F.sum((F.col("coverage") >= 0.95).cast("int")).alias("days_ge_95pct"),
            F.sum((active & (F.col("n_valid") == 0)).cast("int")).alias("active_days_no_data"),
        )
        .withColumn("expected_slots_calendar", F.col("calendar_days") * SLOTS_PER_DAY)
        .withColumn("expected_slots_active", F.col("active_days") * SLOTS_PER_DAY)
        .withColumn("coverage_calendar", F.col("n_valid") / F.col("expected_slots_calendar"))
        .withColumn("coverage_active", F.when(F.col("expected_slots_active") > 0,
                    F.col("n_valid") / F.col("expected_slots_active")))
    )
    return out.orderBy(*keys)


def update_latency(clean: DataFrame) -> DataFrame:
    parts = []
    for col in ["update_timestamp", "reading_update_timestamp"]:
        lat = (F.col(col).cast("long") - F.col("timestamp_sgt").cast("long"))
        parts.append(
            clean.withColumn("lat_s", lat).groupBy("year").agg(
                F.lit(col).alias("field"),
                F.count(F.lit(1)).alias("rows"),
                F.sum(F.col(col).isNull().cast("int")).alias("null_rows"),
                F.sum((F.col("lat_s") < 0).cast("int")).alias("negative_rows"),
                F.min("lat_s").alias("min_s"),
                F.percentile_approx("lat_s", [0.5, 0.9, 0.99], 10000).alias("p50_p90_p99_s"),
                F.max("lat_s").alias("max_s"),
                F.avg((F.col("lat_s") > 3600).cast("double")).alias("share_gt_1h"),
                F.avg((F.col("lat_s") > 86400).cast("double")).alias("share_gt_1d"),
            ))
    out = parts[0].unionByName(parts[1])
    return (out.withColumn("p50_s", F.col("p50_p90_p99_s")[0])
            .withColumn("p90_s", F.col("p50_p90_p99_s")[1])
            .withColumn("p99_s", F.col("p50_p90_p99_s")[2])
            .drop("p50_p90_p99_s").orderBy("field", "year"))


def update_ts_equal(clean: DataFrame) -> DataFrame:
    return clean.groupBy("year").agg(
        F.avg((F.col("update_timestamp") == F.col("reading_update_timestamp")).cast("double"))
        .alias("share_update_eq_reading_update"),
        F.countDistinct("available_at_basis").alias("n_available_bases"),
    ).orderBy("year")


def gap_distribution(clean: DataFrame) -> DataFrame:
    """Time between consecutive observations of one station (5 min = no gap)."""
    w = Window.partitionBy("station_id").orderBy("timestamp_sgt")
    diff = (F.col("timestamp_sgt").cast("long")
            - F.lag("timestamp_sgt").over(w).cast("long")) / 60
    bucket = (F.when(F.col("gap_min") == 5, "5min")
              .when(F.col("gap_min") <= 30, "10-30min")
              .when(F.col("gap_min") <= 60, "35-60min")
              .when(F.col("gap_min") <= 360, "1-6h")
              .when(F.col("gap_min") <= 1440, "6-24h")
              .otherwise(">1d"))
    return (clean.select("station_id", "timestamp_sgt", "year")
            .withColumn("gap_min", diff).filter(F.col("gap_min").isNotNull())
            .groupBy("year", bucket.alias("gap_bucket"))
            .agg(F.count(F.lit(1)).alias("n"),
                 F.sum(F.col("gap_min") - 5).alias("missing_minutes"))
            .orderBy("year", "gap_bucket"))


def run_clean(spark, cfg, log) -> None:
    tables = repo_path(cfg, "tables_dir")
    qdir = repo_path(cfg, "quality_dir")
    clean_dir = repo_path(cfg, "clean_dir")
    c = cfg["cleaning"]
    clean = spark.read.parquet(str(clean_dir / "clean_rainfall"))
    stations = spark.read.parquet(str(clean_dir / "stations"))
    days = calendar_days(spark, c["window_start"][:10], c["window_end"][:10])

    day_cov = station_day_coverage(clean, stations, days)
    day_cov.write.mode("overwrite").partitionBy("year").parquet(str(qdir / "station_day_coverage"))
    day_cov = spark.read.parquet(str(qdir / "station_day_coverage"))

    month_cov = roll_up(day_cov, ["station_id", "year", "month"])
    year_cov = roll_up(day_cov, ["station_id", "year"])
    month_cov.write.mode("overwrite").parquet(str(qdir / "station_month_coverage"))
    year_cov.write.mode("overwrite").parquet(str(qdir / "station_year_coverage"))
    write_small_csv(month_cov, tables / "station_month_coverage.csv")
    write_small_csv(year_cov, tables / "station_year_coverage.csv")

    overview = (clean.groupBy("year").agg(
        F.count(F.lit(1)).alias("clean_rows"),
        F.countDistinct("station_id").alias("n_stations"),
        F.avg((F.col("rainfall_mm") == 0).cast("double")).alias("share_zero"),
        F.avg((F.col("rainfall_mm") > 0).cast("double")).alias("share_wet"),
        F.sum((F.col("quality_flag") != "ok").cast("int")).alias("rows_from_multi_version_keys"),
        F.sum((F.col("align_shift_s") != 0).cast("int")).alias("rows_time_snapped"),
    ).orderBy("year"))
    write_small_csv(overview, tables / "clean_overview_by_year.csv")
    write_small_csv(update_latency(clean), tables / "update_latency_summary.csv")
    write_small_csv(update_ts_equal(clean), tables / "update_timestamp_agreement.csv")
    write_small_csv(gap_distribution(clean), tables / "gap_distribution.csv")
    for r in overview.collect():
        log.info("clean %s", r.asDict())


def main() -> int:
    parser = base_arg_parser(__doc__)
    parser.add_argument("--stage", choices=["raw", "clean"], required=True)
    args = parser.parse_args()
    cfg = load_config(args.config)
    log = setup_logging(cfg, f"data_quality_{args.stage}")
    spark = get_spark(cfg, f"dsa5208-quality-{args.stage}")
    (run_raw if args.stage == "raw" else run_clean)(spark, cfg, log)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
