import hashlib
import json
from pathlib import Path
from datetime import date, datetime, timezone
import calendar
import yaml
from pyspark.sql import SparkSession, functions as F, types as T

CLEAN = 'station_id string, timestamp_sgt string, rainfall_mm double, quality_flag string'
STATIONS = 'station_id string, latitude double, longitude double, comparable boolean'
PREDICTIONS = ('run_id string, model_name string, base_sample_id string, station_id string, '
               'prediction_time string, threshold_mm double, label double, probability double, '
               'split string, inference_mode string')


def load_config(path):
    cfg = yaml.safe_load(Path(path).read_text())
    start, end = date.fromisoformat(cfg['start']), date.fromisoformat(cfg['end'])
    if start.day != 1 or end.day != 1 or start >= end:
        raise ValueError('Analysis interval must be nonempty, month aligned, end exclusive')
    for x in cfg['coverage_thresholds']:
        if not 0 < x <= 1:
            raise ValueError('Coverage thresholds must be in (0,1]')
    if cfg['primary_coverage'] not in cfg['coverage_thresholds']:
        raise ValueError('primary_coverage must be in coverage_thresholds')
    if cfg.get('reference_annual_coverage') is not None:
        if cfg['reference_annual_coverage'] not in cfg['reference_thresholds'] or any(not 0 < t <= 1 for t in cfg['reference_thresholds']):
            raise ValueError('Invalid reference coverage thresholds')
    if not 0 <= cfg['alert_probability'] <= 1:
        raise ValueError('alert_probability must be in [0,1]')
    if not 1 <= cfg['reliability_bins'] <= 100:
        raise ValueError('reliability_bins must be between 1 and 100')
    return cfg


def session():
    return (SparkSession.builder.appName('DSA5208-member-B')
            .config('spark.sql.session.timeZone', 'Asia/Singapore')
            .getOrCreate())


def reject(df, condition, message):
    if df.where(condition).limit(1).count():
        raise ValueError(message)


def unique(df, keys):
    if df.groupBy(*keys).count().where('count > 1').limit(1).count():
        raise ValueError(f'Duplicate key: {keys}')


def read_table(spark, path, ddl):
    schema = T._parse_datatype_string(ddl)
    if str(path).endswith('.csv'):
        df = spark.read.option('header', True).option('mode', 'FAILFAST').option('enforceSchema', False).schema(schema).csv(str(path))
    else:
        df = spark.read.parquet(str(path))
        missing = set(schema.fieldNames()) - set(df.columns)
        if missing:
            raise ValueError(f'Missing fields: {missing}')
        # The adapter accepts upstream timestamps as ISO strings or Spark timestamps.
        df = df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema])
    if not df.take(1):
        raise ValueError(f'Empty input: {path}')
    return df


def parse_time(df, column):
    # Offset-bearing strings are converted to SGT; naive strings are explicitly
    # interpreted as SGT by the provisional contract and session timezone.
    reject(df, F.col(column).isNull(), f'Missing {column}')
    df = df.withColumn(column, F.to_timestamp(column))
    reject(df, F.col(column).isNull() | (F.unix_timestamp(column) % 300 != 0),
           f'Invalid or non-five-minute timestamp: {column}; ask A/C to repair upstream')
    return df


def finite(column):
    return F.col(column).isNotNull() & ~F.isnan(column) & (F.abs(F.col(column)) != float('inf'))


def month_calendar(spark, cfg):
    start, end = date.fromisoformat(cfg['start']), date.fromisoformat(cfg['end'])
    rows = []
    d = start
    while d < end:
        rows.append((d.year, d.month, calendar.monthrange(d.year, d.month)[1] * 288))
        d = date(d.year + (d.month == 12), d.month % 12 + 1, 1)
    return spark.createDataFrame(rows, 'year int, month int, expected_slots long')


def write_tables(tables, output):
    # Distributed output; deliberately no full-data collect/toPandas/coalesce(1).
    for name, df in tables.items():
        df.write.mode('errorifexists').parquet(str(Path(output) / name))


def manifest(output, cfg, inputs, kind, fixture):
    Path(output).mkdir(parents=True, exist_ok=True)
    content = {'kind': kind, 'fixture_only': fixture, 'created_utc': datetime.now(timezone.utc).isoformat(),
               'config': cfg, 'inputs': inputs, 'data_version': cfg['data_version'],
               'status': 'SYNTHETIC - NOT REPORT RESULTS' if fixture else 'REAL INPUT - REVIEW REQUIRED'}
    content['config_sha256'] = hashlib.sha256(json.dumps(cfg, sort_keys=True).encode()).hexdigest()
    (Path(output) / 'manifest.json').write_text(json.dumps(content, indent=2, ensure_ascii=False))


def check_mode(cfg, fixture):
    if bool(cfg.get('fixture_only')) != fixture:
        raise ValueError('Config fixture_only and --fixture must agree')
    if not fixture and (not cfg.get('contract_confirmed') or cfg['data_version'] == 'PENDING'):
        raise ValueError('Real run requires confirmed contract and actual data_version')
