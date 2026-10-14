"""Spark EDA (探索性数据分析); consumes A's cleaned table, never cleans raw data."""
from pyspark.sql import functions as F
from .common import reject, unique, finite, parse_time, month_calendar


def summary(df, keys):
    return (df.groupBy(*keys).agg(F.count('*').alias('valid_slots'),
            F.sum('rainfall_mm').alias('observed_rainfall_mm'),
            F.sum((F.col('rainfall_mm') > 0).cast('long')).alias('wet_slots'))
            .withColumn('wet_frequency', F.col('wet_slots') / F.col('valid_slots'))
            .withColumn('mean_mm_per_valid_slot', F.col('observed_rainfall_mm') / F.col('valid_slots'))
            .withColumn('wet_mean_mm', F.when(F.col('wet_slots') > 0,
                        F.col('observed_rainfall_mm') / F.col('wet_slots'))))


def validate(clean, stations):
    reject(stations, F.col('station_id').isNull() | (F.trim('station_id') == '') |
           F.col('comparable').isNull() | ~finite('latitude') | ~finite('longitude') |
           ~F.col('latitude').between(-90, 90) | ~F.col('longitude').between(-180, 180),
           'Invalid station metadata')
    unique(stations, ['station_id'])
    reject(clean, F.col('station_id').isNull() | F.col('quality_flag').isNull() |
           ~F.col('quality_flag').isin('valid', 'invalid', 'missing'), 'Invalid clean-table contract')
    clean = parse_time(clean, 'timestamp_sgt')
    unique(clean, ['station_id', 'timestamp_sgt'])
    if clean.join(stations, 'station_id', 'left_anti').limit(1).count():
        raise ValueError('Station missing from metadata')
    reject(clean, (F.col('quality_flag') == 'valid') &
           (~finite('rainfall_mm') | (F.col('rainfall_mm') < 0)), 'Invalid valid rainfall; repair upstream')
    return clean


def fixed_stations(monthly, stations, threshold, months):
    return (monthly.groupBy('station_id').agg(F.min('coverage').alias('min_coverage'),
            F.count('*').alias('months')).join(stations.where('comparable'), 'station_id')
            .where((F.col('min_coverage') >= threshold) & (F.col('months') == months))
            .select('station_id'))


def station_year(monthly):
    return (monthly.groupBy('station_id','year').agg(
        F.sum('observed_rainfall_mm').alias('observed_rainfall_mm'),
        F.sum('valid_slots').alias('valid_slots'),F.sum('wet_slots').alias('wet_slots'),
        F.sum('expected_slots').alias('expected_slots'))
        .withColumn('coverage',F.col('valid_slots')/F.col('expected_slots'))
        .withColumn('wet_frequency',F.col('wet_slots')/F.col('valid_slots'))
        .withColumn('mean_mm_per_valid_slot',F.col('observed_rainfall_mm')/F.col('valid_slots')))


def reference_stations(yearly, stations, threshold, years):
    return (yearly.groupBy('station_id').agg(F.min('coverage').alias('min_annual_coverage'),
            F.count('*').alias('years')).join(stations.where('comparable'),'station_id')
            .where((F.col('min_annual_coverage')>=threshold)&(F.col('years')==years))
            .select('station_id'))


def across_stations(frame, keys):
    return frame.groupBy(*keys).agg(
        F.avg('observed_rainfall_mm').alias('station_mean_observed_mm'),
        F.avg('wet_frequency').alias('station_mean_wet_frequency'),
        F.avg('mean_mm_per_valid_slot').alias('station_mean_mm_per_valid_slot'),
        F.min('coverage').alias('min_coverage'),F.count('*').alias('station_count'))


def analyze(spark, clean, stations, cfg):
    from pyspark import StorageLevel
    clean = validate(clean, stations)
    scoped = clean.where((F.col('timestamp_sgt') >= F.to_timestamp(F.lit(cfg['start']))) &
                         (F.col('timestamp_sgt') < F.to_timestamp(F.lit(cfg['end']))))
    valid = (scoped.where("quality_flag = 'valid'")
             .withColumn('year', F.year('timestamp_sgt')).withColumn('month', F.month('timestamp_sgt'))
             .withColumn('day', F.to_date('timestamp_sgt')).withColumn('hour', F.hour('timestamp_sgt'))
             .persist(StorageLevel.DISK_ONLY))
    calendar = month_calendar(spark, cfg)
    nmonths = calendar.count()
    grid = stations.select('station_id').crossJoin(calendar)
    monthly = (grid.join(summary(valid, ['station_id', 'year', 'month']),
                        ['station_id', 'year', 'month'], 'left')
               .fillna(0, subset=['valid_slots', 'wet_slots'])
               .withColumn('coverage', F.col('valid_slots') / F.col('expected_slots'))
               .withColumn('passes_primary',F.col('coverage')>=cfg['primary_coverage'])
               .persist(StorageLevel.DISK_ONLY))
    reject(monthly, F.col('coverage') > 1, 'Coverage > 1; check upstream uniqueness/time grid')
    yearly=station_year(monthly).persist(StorageLevel.DISK_ONLY)
    daily=summary(valid,['station_id','day']).withColumn('expected_slots',F.lit(288))
    daily=daily.withColumn('coverage',F.col('valid_slots')/288)
    tables={'station_month':monthly,'station_year':yearly,'station_day_observed':daily,
            'quality_counts':scoped.groupBy('quality_flag').count()}
    sensitivity=[]
    for threshold in cfg['coverage_thresholds']:
        selected=fixed_stations(monthly,stations,threshold,nmonths)
        sensitivity.append((float(threshold),selected.count(),nmonths))
        tables['annual_'+str(round(threshold*100))]=across_stations(yearly.join(selected,'station_id'),['year'])
        if threshold==cfg['primary_coverage']:
            primary_ids=selected
            tables['fixed_stations']=selected.join(stations,'station_id')
    tables['sensitivity']=spark.createDataFrame(sensitivity,
        'coverage_threshold double, station_count long, required_months long')
    if cfg.get('reference_annual_coverage') is not None:
        if not cfg['start'].endswith('-01-01') or not cfg['end'].endswith('-01-01'):
            raise ValueError('Reference annual rules require full calendar years')
        years=calendar.select('year').distinct().count()
        reference=[]
        for threshold in cfg['reference_thresholds']:
            ids=reference_stations(yearly,stations,threshold,years)
            reference.append((float(threshold),ids.count(),years))
            tables['reference_annual_'+str(round(threshold*100))]=across_stations(yearly.join(ids,'station_id'),['year'])
            if threshold==cfg['reference_annual_coverage']:
                primary_ids=ids
        tables['reference_sensitivity']=spark.createDataFrame(reference,
            'coverage_threshold double, station_count long, required_years long')
    tables['analysis_stations']=primary_ids.join(stations,'station_id')
    selected_monthly=monthly.join(primary_ids,'station_id')
    tables['year_month']=across_stations(selected_monthly,['year','month'])
    primary=valid.join(primary_ids,'station_id').persist(StorageLevel.DISK_ONLY)
    station_hour=summary(primary,['station_id','hour'])
    tables['station_hour']=station_hour
    tables['hourly']=station_hour.groupBy('hour').agg(
        F.avg('wet_frequency').alias('station_mean_wet_frequency'),
        F.avg('mean_mm_per_valid_slot').alias('station_mean_mm_per_valid_slot'),
        F.avg('wet_mean_mm').alias('station_mean_wet_mm'),
        F.count('*').alias('station_count'),F.count('wet_mean_mm').alias('wet_station_count'))
    tables['month_hour']=summary(primary,['station_id','month','hour'])
    tables['spatial']=summary(primary,['station_id']).join(stations,'station_id')
    tables['overview_year']=valid.groupBy('year').agg(F.count('*').alias('valid_slots'),
        F.countDistinct('station_id').alias('station_count'),
        F.avg((F.col('rainfall_mm')==0).cast('double')).alias('share_zero'))
    return tables
