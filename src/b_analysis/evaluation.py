"""Independent Spark checks of C's predictions, without constructing labels."""
from pyspark.sql import functions as F, Window
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from .common import reject, unique, finite, parse_time

GROUPS = ['run_id', 'model_name', 'split', 'threshold_mm']


def validate(pred):
    required = ['run_id', 'model_name', 'base_sample_id', 'station_id', 'split', 'inference_mode']
    for column in required:
        reject(pred, F.col(column).isNull() | (F.trim(column) == ''), f'Missing {column}')
    reject(pred, ~finite('probability') | ~F.col('probability').between(0, 1) |
           ~finite('label') | ~F.col('label').isin(0., 1.) |
           ~finite('threshold_mm') | (F.col('threshold_mm') < 0), 'Invalid prediction values')
    reject(pred, ~F.col('split').isin('train', 'validation', 'calibration', 'test'), 'Unknown split')
    pred = parse_time(pred, 'prediction_time')
    unique(pred, ['run_id', 'model_name', 'base_sample_id', 'threshold_mm'])
    unique(pred, ['run_id', 'model_name', 'station_id', 'prediction_time', 'threshold_mm'])
    # One physical forecast origin must not acquire a new ID to cross splits.
    for keys in [['base_sample_id'], ['station_id', 'prediction_time']]:
        if (pred.groupBy(*keys).agg(F.countDistinct('split').alias('n'))
                .where('n > 1').limit(1).count()):
            raise ValueError('Base sample leaks across splits')
    identity = pred.groupBy('base_sample_id').agg(
        F.countDistinct(F.struct('station_id', 'prediction_time')).alias('n'))
    reject(identity, F.col('n') > 1, 'Inconsistent base_sample_id identity')
    labels = pred.groupBy('station_id', 'prediction_time', 'threshold_mm').agg(
        F.countDistinct('label').alias('n'))
    reject(labels, F.col('n') > 1, 'Models/runs disagree on reference labels')
    return pred


def evaluate(pred, cfg):
    pred = validate(pred)
    if 'decision_threshold' not in pred.columns:
        pred = pred.withColumn('decision_threshold',F.lit(cfg['alert_probability']))
    reject(pred,~finite('decision_threshold') | ~F.col('decision_threshold').between(0,1),'Invalid decision threshold')
    reject(pred.groupBy(*GROUPS).agg(F.countDistinct('decision_threshold').alias('n')),
           F.col('n')>1,'Threshold must be frozen and constant within each metric group')
    cutoff = F.col('decision_threshold')
    eps = 1e-15
    pred = pred.withColumn('p_clipped', F.greatest(F.lit(eps),
                           F.least(F.lit(1-eps), F.col('probability'))))
    alert = F.col('probability') >= cutoff
    positive = F.col('label') == 1
    metrics = pred.groupBy(*GROUPS).agg(
        F.first('decision_threshold').alias('alert_probability'),
        F.count('*').alias('n'), F.sum('label').cast('long').alias('positive_count'),
        F.avg('label').alias('positive_rate'),
        F.avg(F.pow(F.col('probability') - F.col('label'), 2)).alias('brier'),
        F.avg(-F.col('label') * F.log('p_clipped') - (1-F.col('label')) *
              F.log(1-F.col('p_clipped'))).alias('log_loss'),
        F.sum((alert & positive).cast('long')).alias('tp'),
        F.sum((alert & ~positive).cast('long')).alias('fp'),
        F.sum((~alert & ~positive).cast('long')).alias('tn'),
        F.sum((~alert & positive).cast('long')).alias('fn'))
    metrics = (metrics.withColumn('precision', F.when(F.col('tp')+F.col('fp') > 0,
                           F.col('tp')/(F.col('tp')+F.col('fp'))))
               .withColumn('recall', F.when(F.col('tp')+F.col('fn') > 0,
                           F.col('tp')/(F.col('tp')+F.col('fn'))))
               .withColumn('f1', F.when(2*F.col('tp')+F.col('fp')+F.col('fn') > 0,
                           2*F.col('tp')/(2*F.col('tp')+F.col('fp')+F.col('fn')))))
    # Bounded metadata only, not prediction rows; fail before unbounded driver collection.
    groups = metrics.select(*GROUPS, 'n', 'positive_count').limit(201).collect()
    if len(groups) > 200:
        raise ValueError('More than 200 metric groups; split evaluation into smaller batches')
    auc_rows = []
    for group in groups:
        subset = pred
        for key in GROUPS:
            subset = subset.where(F.col(key) == group[key])
        roc = pr = None
        if 0 < group.positive_count < group.n:
            evaluator = BinaryClassificationEvaluator(rawPredictionCol='probability', labelCol='label', numBins=0)
            roc = evaluator.setMetricName('areaUnderROC').evaluate(subset)
            pr = evaluator.setMetricName('areaUnderPR').evaluate(subset)
        auc_rows.append(tuple(group[k] for k in GROUPS) + (roc, pr))
    auc = pred.sparkSession.createDataFrame(auc_rows,
        'run_id string, model_name string, split string, threshold_mm double, roc_auc double, pr_auc double')
    metrics = metrics.join(auc, GROUPS)
    bins = cfg['reliability_bins']
    reliability = (pred.withColumn('bin', F.least(F.lit(bins-1), F.floor(F.col('probability')*bins)))
                   .groupBy(*GROUPS, 'bin').agg(F.count('*').alias('n'),
                    F.avg('probability').alias('mean_probability'), F.avg('label').alias('event_rate'))
                   .withColumn('bin_lower', F.col('bin')/bins)
                   .withColumn('bin_upper', (F.col('bin')+1)/bins))
    window = Window.partitionBy('run_id', 'model_name', 'base_sample_id').orderBy('threshold_mm')
    monotonic = (pred.withColumn('previous_p', F.lag('probability').over(window))
                 .withColumn('previous_y', F.lag('label').over(window))
                 .groupBy('run_id', 'model_name', 'split').agg(
                 F.count('previous_p').alias('adjacent_pairs'),
                 F.sum(F.coalesce(F.col('probability') > F.col('previous_p') + 1e-12,
                                  F.lit(False)).cast('long')).alias('probability_violations'),
                 F.sum(F.coalesce(F.col('label') > F.col('previous_y'),
                                  F.lit(False)).cast('long')).alias('label_violations')))
    by_month = (pred.withColumn('month', F.month('prediction_time')).groupBy(*GROUPS, 'month')
                .agg(F.count('*').alias('n'), F.avg('label').alias('positive_rate'),
                     F.avg(F.pow(F.col('probability')-F.col('label'), 2)).alias('brier')))
    return {'metrics': metrics, 'reliability': reliability, 'monotonicity': monotonic,
            'evaluation_month': by_month}
