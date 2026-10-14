"""Explicit adapters for A data and C prediction interchange v0.1/v0.2."""
from pyspark.sql import functions as F
from .common import reject, unique, finite, parse_time


def read_named(spark, path):
    if str(path).endswith('.csv'):
        return spark.read.option('header',True).option('mode','FAILFAST').csv(str(path))
    return spark.read.parquet(str(path))


def require(df, names):
    missing=set(names)-set(df.columns)
    if missing: raise ValueError(f'Missing upstream columns: {sorted(missing)}')


def adapt_a(clean, metadata):
    require(clean,['station_id','timestamp_sgt','rainfall_mm','quality_flag'])
    reject(clean,F.col('quality_flag').isNull() |
           ~F.col('quality_flag').isin('ok','merged_same_value','conflict_resolved_latest'),
           'Unknown A quality flag: update contract explicitly')
    observations=clean.select('station_id','timestamp_sgt',F.col('rainfall_mm').cast('double'),
                              F.lit('valid').alias('quality_flag'))
    require(metadata,['station_id','n_locations','n_devices','latest_latitude','latest_longitude'])
    stations=metadata.select('station_id',F.col('latest_latitude').cast('double').alias('latitude'),
        F.col('latest_longitude').cast('double').alias('longitude'),
        ((F.col('n_locations').cast('int')==1)&(F.col('n_devices').cast('int')==1)).alias('comparable'))
    # Location/device stability is inferred from A's audit, not official station history.
    return observations,stations


def adapt_c(frame, fixture, system_id=None):
    """Keep publication semantics; optionally evaluate a mixed fallback system.

    system_id combines serving rows without changing their source model_id.
    Legacy v0.1 fixtures may omit inference_mode; v0.2 values are preserved.
    """
    required=['station_id','prediction_time','threshold_mm','label','label_available',
              'raw_probability','calibrated_probability','decision_threshold','probability_source',
              'prediction','split','model_id','feature_complete','is_fixture']
    require(frame,required)
    df=frame
    for name in ['threshold_mm','label','raw_probability','calibrated_probability','decision_threshold','prediction']:
        df=df.withColumn(name,F.col(name).cast('double'))
    for name in ['label_available','is_fixture','feature_complete']:
        df=df.withColumn(name,F.col(name).cast('boolean'))
    for name in ['station_id','model_id','split']:
        reject(df,F.col(name).isNull() | (F.trim(name)==''),f'Missing C {name}')
    reject(df,F.col('is_fixture').isNull() | (F.col('is_fixture')!=F.lit(fixture)),
           'C fixture marker does not match run mode')
    reject(df,F.col('label_available').isNull() | F.col('feature_complete').isNull(), 'Invalid C boolean')
    reject(df,~finite('threshold_mm') | (F.col('threshold_mm')<0) |
           ~finite('decision_threshold') | ~F.col('decision_threshold').between(0,1), 'Invalid C threshold')
    reject(df,F.col('probability_source').isNull() | ~F.col('probability_source').isin('raw','calibrated'),
           'Unknown C probability_source')
    reject(df,F.col('label_available') & (~finite('label') | ~F.col('label').isin(0.,1.)),
           'Available C target must have binary label')
    df=parse_time(df,'prediction_time')
    unique(df,['model_id','split','station_id','prediction_time','threshold_mm'])
    if system_id is not None:
        if not isinstance(system_id, str) or not system_id.strip():
            raise ValueError('system_id must be a non-empty string')
        unique(df,['split','station_id','prediction_time','threshold_mm'])
    if 'inference_mode' not in df.columns:
        df=df.withColumn('inference_mode',F.lit('unspecified_by_C'))
    reject(df,F.col('inference_mode').isNull() | (F.trim('inference_mode')==''),
           'Missing C inference_mode')
    run=F.lit(system_id) if system_id is not None else F.col('model_id')
    selected=F.when(F.col('probability_source')=='raw',F.col('raw_probability')).otherwise(F.col('calibrated_probability'))
    df=df.withColumn('_selected',selected)
    selected_ok=finite('_selected') & F.col('_selected').between(0,1)
    reject(df,selected_ok & (F.col('prediction').isNull() |
           (F.col('prediction')!=(F.col('_selected')>=F.col('decision_threshold')).cast('double'))),
           'C prediction disagrees with probability_source/decision_threshold')
    outputs=[]; audits=[]
    for source in ['raw','calibrated']:
        part=df.withColumn('probability',F.col(source+'_probability'))
        usable=F.col('label_available') & finite('probability') & F.col('probability').between(0,1)
        part=part.withColumn('exclusion_reason',
            F.when(~F.col('label_available'),'label_unavailable')
             .when(~finite('probability') | ~F.col('probability').between(0,1),'probability_unavailable_or_invalid')
             .otherwise('retained'))
        audits.append(part.groupBy('model_id','split','threshold_mm','exclusion_reason').count()
                      .withColumn('probability_version',F.lit(source)))
        outputs.append(part.where(usable).select(
            run.alias('run_id'),F.concat_ws(':',run,F.lit(source)).alias('model_name'),
            F.sha2(F.to_json(F.struct('station_id','prediction_time')),256).alias('base_sample_id'),
            'station_id','prediction_time','threshold_mm','label','probability','split',
            'inference_mode','decision_threshold','feature_complete',
            F.col('model_id').alias('source_model_id')))
    return outputs[0].unionByName(outputs[1]),audits[0].unionByName(audits[1])
