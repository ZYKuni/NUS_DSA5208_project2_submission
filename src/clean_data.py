"""P1-02/P1-03: parse, align, validate, de-duplicate and build station metadata.

Usage:
    python src/clean_data.py --config configs/project.yaml [--skip-parse] [--write-samples]

Every raw row receives exactly one `fate`, so the cleaning log always
reconciles (raw rows = sum of fates, per source year):

    kept                 -> data/clean/clean_rainfall (key: station_id, timestamp_sgt)
    exact_duplicate      -> dropped (identical raw row already kept)
    same_value_merged    -> dropped (same key and value, older version)
    conflict_superseded  -> dropped (same key, different value, older version)
    conflict_unresolved  -> data/clean/quarantine (cannot tell which value is current)
    <reject reason>      -> data/clean/quarantine (see REJECT_ORDER)

Missing slots are never filled: an absent row means "not observed", not 0 mm.
"""
from __future__ import annotations

import os
import sys

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common import (RAW_COLUMNS, base_arg_parser, get_spark, load_config, raw_schema,
                    repo_path, setup_logging, write_small_csv)

TS_PATTERNS = ["yyyy-MM-dd'T'HH:mm:ssXXX", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"]

# First matching reason wins; order is from structural to value-level problems.
REJECT_ORDER = [
    "corrupt_record", "station_missing", "ts_unparseable", "unaligned_timestamp",
    "out_of_window", "type_unit_unexpected", "value_missing", "value_non_numeric",
    "value_negative", "value_precision",
]

CLEAN_COLUMNS = [
    "station_id", "raw_timestamp", "timestamp_sgt", "rainfall_mm", "station_name",
    "station_device_id", "latitude", "longitude", "update_timestamp",
    "reading_update_timestamp", "available_at", "available_at_basis", "align_shift_s",
    "value_rounded", "n_raw_versions", "quality_flag", "source_file", "source_year", "year", "month",
    # C task2 contract (docs/C/task2_contract.md): canonical names for labels.py
    "event_time", "quality_ok",
]


def parse_ts(col: str):
    return F.coalesce(*[F.to_timestamp(F.col(col), p) for p in TS_PATTERNS])


def read_raw(spark, paths: list[str]) -> DataFrame:
    df = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("multiLine", False)
        .schema(raw_schema())
        .csv(paths)
    )
    return (
        df.withColumn("source_file", F.element_at(F.split(F.input_file_name(), "/"), -1))
        .withColumn("source_year",
                    F.regexp_extract("source_file", r"(\d{4})", 1).cast("int"))
    )


def parse_raw(df: DataFrame, cfg: dict) -> DataFrame:
    """Add typed columns, the aligned slot and a single `reject_reason`."""
    c = cfg["cleaning"]
    slot = int(c["slot_seconds"])
    up, down = int(c["snap_up_max_seconds"]), int(c["snap_down_max_seconds"])
    dec = c["value_decimal"]

    df = (
        df.withColumn("ts_obs", parse_ts("timestamp"))
        .withColumn("update_ts", parse_ts("update_timestamp"))
        .withColumn("reading_update_ts", parse_ts("reading_update_timestamp"))
        .withColumn("epoch", F.col("ts_obs").cast("long"))
        # SGT is UTC+8 = 96 slots, so epoch % 300 is the same in UTC and SGT.
        .withColumn("slot_offset_s", F.pmod(F.col("epoch"), F.lit(slot)))
        .withColumn(
            "slot_epoch",
            F.when(F.col("slot_offset_s") == 0, F.col("epoch"))
            .when(F.col("slot_offset_s") >= slot - up,
                  F.col("epoch") - F.col("slot_offset_s") + slot)
            .when(F.col("slot_offset_s") <= down, F.col("epoch") - F.col("slot_offset_s")),
        )
        .withColumn("slot_ts", F.col("slot_epoch").cast("timestamp"))
        .withColumn("align_shift_s", (F.col("slot_epoch") - F.col("epoch")).cast("int"))
        .withColumn("value_str", F.trim(F.col("reading_value")))
        .withColumn("value_dec", F.col("value_str").cast(dec))  # half-up to scale
        .withColumn("value_rounded", F.coalesce(F.col("value_str").rlike(r"\.\d{4,}"), F.lit(False)))
        .withColumn("latitude", F.col("location_latitude").cast("double"))
        .withColumn("longitude", F.col("location_longitude").cast("double"))
        .withColumn("date_col_mismatch",
                    F.col("date").isNotNull() & F.col("ts_obs").isNotNull()
                    & (F.to_date("date") != F.to_date("ts_obs")))
    )

    in_window = (F.col("slot_ts") >= F.to_timestamp(F.lit(c["window_start"]))) & (
        F.col("slot_ts") < F.to_timestamp(F.lit(c["window_end"])))
    checks = {
        "corrupt_record": F.col("_corrupt_record").isNotNull(),
        "station_missing": F.col("station_id").isNull() | (F.trim("station_id") == ""),
        "ts_unparseable": F.col("ts_obs").isNull(),
        "unaligned_timestamp": F.col("slot_epoch").isNull(),
        "out_of_window": ~in_window,
        "type_unit_unexpected": ~F.col("reading_type").isin(c["allowed_reading_types"])
        | ~F.col("reading_unit").isin(c["allowed_units"])
        | F.col("reading_type").isNull() | F.col("reading_unit").isNull(),
        "value_missing": F.col("value_str").isNull() | (F.col("value_str") == ""),
        "value_non_numeric": F.col("value_dec").isNull(),
        "value_negative": F.col("value_dec") < 0,
        # more decimals than the Decimal type keeps; only rejected when
        # cleaning.excess_precision == "quarantine" (default: round half-up, flag)
        "value_precision": F.col("value_rounded")
        & F.lit(c.get("excess_precision", "round") == "quarantine"),
    }
    reason = F.lit(None).cast("string")
    for name in reversed(REJECT_ORDER):
        reason = F.when(checks[name], F.lit(name)).otherwise(reason)
    return df.withColumn("reject_reason", reason)


def assign_fates(parsed: DataFrame) -> DataFrame:
    """Give every row one `fate` (see module docstring) plus key-group stats."""
    valid = parsed.filter(F.col("reject_reason").isNull())
    rejected = parsed.filter(F.col("reject_reason").isNotNull()).withColumn(
        "fate", F.col("reject_reason"))

    # 1. Exact duplicates of the full raw row (e.g. the same line in two files).
    w_exact = Window.partitionBy(*RAW_COLUMNS).orderBy("source_file")
    valid = valid.withColumn("_exact_rank", F.row_number().over(w_exact))
    exact_dup = valid.filter(F.col("_exact_rank") > 1).withColumn(
        "fate", F.lit("exact_duplicate"))
    uniq = valid.filter(F.col("_exact_rank") == 1)

    # 2. Versions sharing (station_id, slot_ts). Newest = max(reading_update_ts,
    #    update_ts). A conflict is resolvable only if all newest rows agree.
    w_key = Window.partitionBy("station_id", "slot_ts")
    version = F.struct(F.col("reading_update_ts"), F.col("update_ts"))
    w_order = w_key.orderBy(F.col("reading_update_ts").desc_nulls_last(),
                            F.col("update_ts").desc_nulls_last(),
                            F.col("source_file").desc())
    uniq = (
        uniq.withColumn("n_raw_versions", F.count(F.lit(1)).over(w_key))
        .withColumn("_n_values", F.size(F.collect_set("value_dec").over(w_key)))
        .withColumn("_newest", F.max(version).over(w_key))
        .withColumn("_is_newest", version == F.col("_newest"))
        .withColumn("_n_newest_values", F.size(F.collect_set(
            F.when(F.col("_is_newest"), F.col("value_dec"))).over(w_key)))
        .withColumn("_rank", F.row_number().over(w_order))
    )
    conflict = F.col("_n_values") > 1
    fate = (
        F.when(conflict & (F.col("_n_newest_values") > 1), "conflict_unresolved")
        .when(F.col("_rank") > 1,
              F.when(conflict, "conflict_superseded").otherwise("same_value_merged"))
        .otherwise("kept")
    )
    quality = (F.when(conflict, "conflict_resolved_latest")
               .when(F.col("n_raw_versions") > 1, "merged_same_value")
               .otherwise("ok"))
    uniq = (uniq.withColumn("fate", fate)
            .withColumn("quality_flag", quality)
            .withColumn("is_conflict_group", conflict))

    drop_cols = [c for c in uniq.columns if c.startswith("_")]
    return (
        uniq.drop(*drop_cols)
        .unionByName(exact_dup.drop("_exact_rank"), allowMissingColumns=True)
        .unionByName(rejected, allowMissingColumns=True)
    )


def to_clean(fated: DataFrame) -> DataFrame:
    return (
        fated.filter(F.col("fate") == "kept")
        .withColumn("raw_timestamp", F.col("timestamp"))
        .withColumn("timestamp_sgt", F.col("slot_ts"))
        .withColumn("rainfall_mm", F.col("value_dec"))
        .withColumn("update_timestamp", F.col("update_ts"))
        .withColumn("reading_update_timestamp", F.col("reading_update_ts"))
        # Conservative proxy for when the value was first visible; only the final
        # version exists in the archive, so this may be later than the real release.
        .withColumn("available_at", F.greatest("slot_ts", "update_ts", "reading_update_ts"))
        .withColumn("available_at_basis",
                    F.when(F.col("update_ts").isNull() & F.col("reading_update_ts").isNull(),
                           "slot_only").otherwise("update_timestamps"))
        .withColumn("year", F.year("slot_ts"))
        .withColumn("month", F.month("slot_ts"))
        # Interval-end slot; every kept row is a valid observation (quarantined
        # rows never reach the clean table), so quality_ok is true throughout.
        .withColumn("event_time", F.col("slot_ts"))
        .withColumn("quality_ok", F.col("quality_flag").isin(
            "ok", "merged_same_value", "conflict_resolved_latest"))
        .select(*CLEAN_COLUMNS)
    )


def station_tables(clean: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Per-(station, name, device, location) phases and a per-station summary."""
    phases = (
        clean.groupBy("station_id", "station_name", "station_device_id",
                      "latitude", "longitude")
        .agg(F.min("timestamp_sgt").alias("first_obs"),
             F.max("timestamp_sgt").alias("last_obs"),
             F.count(F.lit(1)).alias("n_obs"))
        .orderBy("station_id", "first_obs")
    )
    summary = (
        phases.groupBy("station_id")
        .agg(F.min("first_obs").alias("first_obs"), F.max("last_obs").alias("last_obs"),
             F.sum("n_obs").alias("n_obs"),
             F.countDistinct("station_name").alias("n_names"),
             F.countDistinct("latitude", "longitude").alias("n_locations"),
             F.countDistinct("station_device_id").alias("n_devices"),
             F.max_by("station_name", "last_obs").alias("latest_name"),
             F.max_by("latitude", "last_obs").alias("latest_latitude"),
             F.max_by("longitude", "last_obs").alias("latest_longitude"))
        .withColumn("has_metadata_change",
                    (F.col("n_names") > 1) | (F.col("n_locations") > 1)
                    | (F.col("n_devices") > 1))
        .orderBy("station_id")
    )
    return phases, summary


def cleaning_log(fated: DataFrame) -> DataFrame:
    fates = ["kept", "exact_duplicate", "same_value_merged", "conflict_superseded",
             "conflict_unresolved"] + REJECT_ORDER
    log = fated.groupBy("source_year").pivot("fate", fates).count().na.fill(0)
    total = sum(F.col(f) for f in fates)
    return log.withColumn("raw_rows", total).orderBy("source_year")


def main() -> int:
    parser = base_arg_parser(__doc__)
    parser.add_argument("--skip-parse", action="store_true",
                        help="reuse data/interim/parsed instead of re-reading the CSVs")
    parser.add_argument("--write-samples", action="store_true")
    args = parser.parse_args()
    cfg = load_config(args.config)
    log = setup_logging(cfg, "clean_data")
    spark = get_spark(cfg, "dsa5208-clean")

    raw_dir = repo_path(cfg, "raw_dir")
    interim = repo_path(cfg, "interim_dir")
    clean_dir = repo_path(cfg, "clean_dir")
    tables = repo_path(cfg, "tables_dir")

    if not args.skip_parse:
        years = sorted(int(y) for y in cfg["source"]["datasets"])
        paths = [str(raw_dir / f"rainfall_{y}.csv") for y in years]
        missing = [p for p in paths if not os.path.exists(p)]
        if missing:
            log.error("missing raw files: %s (run download_data.py first)", missing)
            return 1
        log.info("parsing %d raw files", len(paths))
        parsed = parse_raw(read_raw(spark, paths), cfg)
        (parsed.write.mode("overwrite").partitionBy("source_year")
         .parquet(str(interim / "parsed")))

    parsed = spark.read.parquet(str(interim / "parsed"))
    log.info("assigning fates")
    (assign_fates(parsed).write.mode("overwrite").partitionBy("source_year")
     .parquet(str(interim / "fated")))
    fated = spark.read.parquet(str(interim / "fated"))

    clean = to_clean(fated)
    (clean.repartition("year", "month").sortWithinPartitions("station_id", "timestamp_sgt")
     .write.mode("overwrite").partitionBy("year", "month")
     .parquet(str(clean_dir / "clean_rainfall")))
    clean = spark.read.parquet(str(clean_dir / "clean_rainfall"))

    quarantine_fates = ["conflict_unresolved"] + REJECT_ORDER
    (fated.filter(F.col("fate").isin(quarantine_fates))
     .select(*RAW_COLUMNS, "_corrupt_record", "source_file", "source_year", "fate")
     .write.mode("overwrite").parquet(str(clean_dir / "quarantine")))
    (fated.filter(F.col("is_conflict_group") == True)  # noqa: E712
     .select(*RAW_COLUMNS, "source_file", "slot_ts", "fate")
     .write.mode("overwrite").parquet(str(clean_dir / "conflicts")))

    phases, summary = station_tables(clean)
    summary.write.mode("overwrite").parquet(str(clean_dir / "stations"))
    phases.write.mode("overwrite").parquet(str(clean_dir / "station_phases"))
    write_small_csv(summary, tables / "station_metadata.csv")
    write_small_csv(phases, tables / "station_phases.csv")

    clog = cleaning_log(fated)
    write_small_csv(clog, tables / "cleaning_log.csv")

    extreme = float(cfg["cleaning"]["extreme_review_mm"])
    write_small_csv(
        clean.filter(F.col("rainfall_mm") > extreme)
        .select("station_id", "station_name", "timestamp_sgt", "rainfall_mm", "quality_flag")
        .orderBy(F.col("rainfall_mm").desc()),
        tables / "extreme_candidates.csv")

    # Key uniqueness check on the final table.
    dup_keys = clean.groupBy("station_id", "timestamp_sgt").count().filter("count > 1").count()
    n_clean = clean.count()
    log.info("clean rows: %d; duplicate keys: %d", n_clean, dup_keys)
    for row in clog.collect():
        log.info("cleaning log %s", row.asDict())
    if dup_keys:
        log.error("clean_rainfall key (station_id, timestamp_sgt) is not unique")
        return 1

    if args.write_samples:
        write_samples(spark, cfg, fated, clean, log)
    spark.stop()
    return 0


def write_samples(spark, cfg, fated, clean, log) -> None:
    """Small real-data samples for B and C (H1 hand-off): the two stations
    with the most observations, one week in Jan 2017 and one in Jun 2024."""
    samples = repo_path(cfg, "samples_dir")
    top = [r.station_id for r in clean.groupBy("station_id").count()
           .orderBy(F.col("count").desc(), "station_id").limit(2).collect()]
    windows = [("2017-01-01", "2017-01-08"), ("2024-06-01", "2024-06-08")]
    in_win = None
    for a, b in windows:
        cond = (F.col("timestamp_sgt") >= F.to_timestamp(F.lit(a))) & (
            F.col("timestamp_sgt") < F.to_timestamp(F.lit(b)))
        in_win = cond if in_win is None else in_win | cond
    sel = clean.filter(F.col("station_id").isin(top) & in_win).orderBy(
        "station_id", "timestamp_sgt")
    write_small_csv(sel.drop("year", "month"), samples / "clean_sample.csv")
    raw = fated.filter(F.col("station_id").isin(top)).withColumnRenamed(
        "slot_ts", "timestamp_sgt").filter(in_win)
    write_small_csv(raw.select(*RAW_COLUMNS, "source_file", "fate")
                    .orderBy("station_id", "timestamp"), samples / "raw_sample.csv")
    log.info("samples written for stations %s", top)


if __name__ == "__main__":
    sys.exit(main())
