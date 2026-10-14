"""B independent review of persisted C artifacts, with no training or selection.

Run from repo root: python -m scripts.review_b_models
v1 scores are reproduced. v2 is separately evaluated post-hoc on the already
opened 2024 sample; these are not untouched-holdout generalisation estimates.
Only aggregates and two bounded case windows leave Spark.
"""
import hashlib
import json
import platform
import subprocess
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F
from src.common import get_spark, load_config
from src.b_analysis.adapters import adapt_c
from src.b_analysis.evaluation import evaluate, GROUPS
from src.predict_safe import KNOTS, monotone_curve, interpolate, predict, validate_inputs
from src.task2.run import fingerprints

OUT = Path('reports/b_model_review')
PREDS = Path('data/predictions/c_available_v1')
SGT = ZoneInfo('Asia/Singapore')


def save(name, value):
    path=OUT/name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value,indent=2,ensure_ascii=False,allow_nan=False,default=str)+'\n')


def materialize(tables):
    return {name:[r.asDict() for r in frame.orderBy(*[c for c in GROUPS if c in frame.columns],
               *(['bin'] if 'bin' in frame.columns else []),
               *(['month'] if 'month' in frame.columns else [])).collect()] for name,frame in tables.items()}


def check_reference(actual, expected):
    # Candidate validation exports have raw probabilities only.
    sources={'raw':expected} if 'metrics' in expected else expected
    maxdiff=0.; groups=0
    for source, ref in sources.items():
        rows=[r for r in actual if r['model_name'].endswith(':'+source)]
        assert len(rows)==len(ref['metrics'])
        refs={r['threshold_mm']:r for r in ref['metrics']}
        for row in rows:
            other=refs[row['threshold_mm']]
            for k in ['n','positive_count','tp','fp','tn','fn']:
                assert row[k]==other[k],(source,row['threshold_mm'],k,row[k],other[k])
            for k in ['positive_rate','brier','log_loss','roc_auc','pr_auc','precision','recall','f1']:
                a,b=row[k],other[k]
                if a is None or b is None: assert a is b,(source,k,a,b)
                else:
                    d=abs(a-b);maxdiff=max(maxdiff,d)
                    assert d<1e-12,(source,row['threshold_mm'],k,a,b)
            groups+=1
    assert len(actual)==sum(len(v['metrics']) for v in sources.values())
    return {'metric_groups':groups,'counts_and_confusion_exact':True,'max_absolute_difference':maxdiff}


def postprocess(frame):
    keys=['station_id','prediction_time']
    @F.udf('array<double>')
    def project(values):
        assert [v['threshold_mm'] for v in values]==list(KNOTS)
        return monotone_curve([v['probability'] for v in values])
    curves=(frame.where(F.col('threshold_mm').isin(*KNOTS)).groupBy(*keys)
            .agg(F.sort_array(F.collect_list(F.struct('threshold_mm','probability'))).alias('_knots'))
            .withColumn('_curve',project('_knots')).select(*keys,'_curve'))
    @F.udf('double')
    def at(curve,q): return interpolate(curve,q)
    return frame.join(curves,keys).withColumn('probability',at('_curve','threshold_mm')).drop('_curve')


def diagnose(pred, features):
    df=pred.join(features.select('station_id','prediction_time','count_60','no_recent_data','lag_5','missing_5','count_15','wet_15'),
                 ['station_id','prediction_time'])
    df=(df.withColumn('recent_state',F.when(F.col('missing_5')==1,'unknown_lag5')
         .when(F.col('lag_5')>0,'wet_lag5').otherwise('dry_lag5'))
        .withColumn('known_15m_state',F.when(F.col('count_15')==0,'unknown_15m')
         .when(F.col('wet_15')>0,'observed_wet_15m').otherwise('observed_dry_15m'))
        .withColumn('history_group',F.when(F.col('count_60')==0,'0_known')
         .when(F.col('count_60')<6,'1-5_known').when(F.col('count_60')<10,'6-9_known')
         .otherwise('10-12_known'))
        .withColumn('fallback',F.when(F.col('no_recent_data')==1,'calendar').otherwise('history')))
    result=[]
    for key in ['recent_state','known_15m_state','history_group','fallback','source_model_id']:
        p=F.col('probability'); y=F.col('label'); a=p>=F.col('decision_threshold')
        rows=(df.groupBy(*GROUPS,F.col(key).alias('group_value')).agg(F.count('*').alias('n'),
          F.sum('label').alias('positive_count'),F.avg('label').alias('positive_rate'),
          F.avg((p-y)**2).alias('brier'),F.sum((a & (y==1)).cast('long')).alias('tp'),
          F.sum((a & (y==0)).cast('long')).alias('fp'),
          F.sum((~a & (y==1)).cast('long')).alias('fn'),
          F.sum((~a & (y==0)).cast('long')).alias('tn'))
          .withColumn('recall',F.when(F.col('positive_count')>0,F.col('tp')/F.col('positive_count'))))
        result.extend(dict(r.asDict(),group_dimension=key) for r in rows.collect())
    return sorted(result,key=lambda x:(x['model_name'],x['threshold_mm'],x['group_dimension'],x['group_value']))


def cases_and_reload(spark, serving, v2):
    clean=spark.read.parquet('data/clean/clean_rainfall')
    cases=[];reloads=[]
    # Descriptive error examples, not model/threshold selection: largest rain FN,
    # then most confident FP at q=0; deterministic station/time tie-breaks.
    choices=[('false_negative',serving.where('threshold_mm=10 AND label=1 AND calibrated_probability<0.5')
              .orderBy(F.desc('future_30m_mm'),'station_id','prediction_time')),
             ('false_positive',serving.where('threshold_mm=0 AND label=0 AND calibrated_probability>=0.5')
              .orderBy(F.desc('calibrated_probability'),'station_id','prediction_time'))]
    for kind, selection in choices:
        row=selection.first();assert row is not None
        stamp=serving.where((F.col('station_id')==row.station_id)&(F.col('prediction_time')==row.prediction_time))
        # Collected Spark datetimes are host-local; use epoch explicitly for windows/output.
        epoch=stamp.select(F.col('prediction_time').cast('long')).first()[0]
        t=datetime.fromtimestamp(epoch,SGT)
        obs=(clean.where((F.col('station_id')==row.station_id)&
             F.col('event_time').cast('long').between(epoch-3600,epoch+1800))
             .select('station_id',F.col('event_time').cast('long').alias('event_epoch'),
                     F.col('available_at').cast('long').alias('available_epoch'),
                     'rainfall_mm','quality_ok','available_at_basis').orderBy('event_epoch').collect())
        records=[]
        for o in obs:
            available=(bool(o.quality_ok) and o.available_at_basis=='update_timestamps' and
                       o.available_epoch is not None and o.available_epoch<=epoch and o.event_epoch<=epoch)
            records.append({'offset_minutes':(o.event_epoch-epoch)/60,
                'event_time':datetime.fromtimestamp(o.event_epoch,SGT).isoformat(),
                'rainfall_mm':float(o.rainfall_mm),'available_at_prediction':available,
                'quality_ok':o.quality_ok,'available_epoch':o.available_epoch})
        future=[o for o in obs if epoch<o.event_epoch<=epoch+1800]
        assert len(future)==6 and all(o.quality_ok for o in future)
        total=sum(o.rainfall_mm for o in future)
        assert total==row.future_30m_mm
        batch=v2.where((F.col('station_id')==row.station_id)&
             (F.col('prediction_time').cast('long')==epoch)&(F.col('threshold_mm')==row.threshold_mm)).first()
        response=predict(spark,row.station_id,t.isoformat(),row.threshold_mm)
        assert abs(response['probability']-batch.probability)<1e-12
        assert abs(response['unadjusted_calibrated_probability']-row.calibrated_probability)<1e-12
        cases.append({'kind':kind,'station_id':row.station_id,'prediction_time':t.isoformat(),
            'threshold_mm':row.threshold_mm,'label':row.label,'future_30m_mm':str(total),
            'v1_probability':row.calibrated_probability,'v2_probability':batch.probability,'records':records})
        reloads.append(response)
    # One real fallback origin, chosen by time, without outcome selection.
    fallback=serving.where("model_id LIKE 'M1%' AND threshold_mm=1").orderBy('prediction_time','station_id').first()
    if fallback:
        epoch=serving.where((F.col('station_id')==fallback.station_id)&
          (F.col('prediction_time')==fallback.prediction_time)).select(F.col('prediction_time').cast('long')).first()[0]
        response=predict(spark,fallback.station_id,datetime.fromtimestamp(epoch,SGT).isoformat(),1.)
        expected=v2.where((F.col('station_id')==fallback.station_id)&
          (F.col('prediction_time').cast('long')==epoch)&(F.col('threshold_mm')==1)).first().probability
        assert response['calendar_fallback'] and abs(response['probability']-expected)<1e-12
        reloads.append(response)
    save('error_cases.json',cases)
    save('independent_reload.json',{'passed':True,'performed_on':'B local clean history and checksum-verified saved model',
         'tolerance':1e-12,'outputs':reloads})


def main():
    start=time.time();OUT.mkdir(exist_ok=True,parents=True)
    frozen=json.loads(Path('reports/c_results/frozen_manifest.json').read_text())
    assert frozen['hashes']==fingerprints(),'Frozen C artifacts changed'
    cfg=load_config('configs/project.yaml');cfg['spark'].update(master='local[4]',driver_memory='4g',shuffle_partitions=8)
    spark=get_spark(cfg,'B-independent-model-review');spark.sparkContext.setLogLevel('ERROR')
    checks=[]
    try:
        for split in ['validation','test']:
            for folder in sorted((PREDS/split).iterdir()):
                if not folder.is_dir():continue
                print('Review',split,folder.name,flush=True)
                original=spark.read.parquet(str(folder)).cache()
                pred,audit=adapt_c(original,False,system_id=folder.name)
                pred=pred.cache();pred.count()
                tables=materialize(evaluate(pred,{'reliability_bins':10,'alert_probability':.5}))
                tables['input_audit']=[r.asDict() for r in audit.collect()]
                ref=json.loads(Path(f'reports/c_results/{split}_{folder.name}.json').read_text())
                result=check_reference(tables['metrics'],ref)
                result.update(split=split,model=folder.name,input_rows=original.count(),
                    origin_count=original.select('station_id','prediction_time').distinct().count())
                checks.append(result);save(f'{split}_{folder.name}.json',tables)
                pred.unpersist();original.unpersist()
        serving=spark.read.parquet(str(PREDS/'test'/'serving')).cache()
        v1,_=adapt_c(serving,False,system_id='serving_v1')
        v1=v1.where("model_name='serving_v1:calibrated'").cache()
        # Guard invariants: every retained origin lies within its own observed
        # span because C sampled observed clean anchors, not a calendar grid.
        clean=spark.read.parquet('data/clean/clean_rainfall')
        bounds=clean.groupBy('station_id').agg(F.min('event_time').alias('_first'),F.max('event_time').alias('_last'))
        assert v1.join(bounds,'station_id','left').where(F.col('_first').isNull()|
          (F.col('prediction_time')<F.col('_first'))|(F.col('prediction_time')>F.col('_last'))).count()==0
        assert v1.where((F.year('prediction_time')!=2024)|(F.second('prediction_time')!=0)|
          (F.minute('prediction_time')%5!=0)|~F.col('threshold_mm').between(0,10)).count()==0
        v2=(postprocess(v1).withColumn('run_id',F.lit('serving_v2_posthoc'))
            .withColumn('model_name',F.lit('serving_v2_posthoc:monotone'))).cache()
        assert v2.count()==v1.count()
        print('Review v2 post-hoc',flush=True)
        save('test_serving_v2_posthoc.json',materialize(evaluate(v2,{'reliability_bins':10,'alert_probability':.5})))
        features=spark.read.parquet('data/features/c_available_v1/prepared').where("split='test'").cache()
        assert features.count()==v1.select('station_id','prediction_time').distinct().count()
        assert features.select('station_id','prediction_time').distinct().count()==features.count()
        save('stratified_metrics.json',diagnose(v1.unionByName(v2),features))
        cases_and_reload(spark,serving,v2)
        assert frozen['hashes']==fingerprints()
        save('validation.json',{'passed':True,'checked_at':datetime.now(SGT).isoformat(),
            'source_revision':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
            'executed_by':'B workspace, automated independent review; human sign-off not claimed',
            'v1_checks':checks,'v1_frozen_hashes_unchanged':True,
            'artifact_sha256':'490bc95b6b282b43739fbf7ee8c920cbb1df14e5e572b8ad09f7fcb80aa046cd',
            'v2_protocol':'fixed existing C PAVA/interpolation; same 2024 sample; post-hoc after holdout opened; no fit/selection/tuning',
            'v2_domain_checks_passed':True,'seconds':time.time()-start,
            'environment':{'python':platform.python_version(),'platform':platform.platform(),'spark':spark.version,
                           'master':'local[4]','driver_memory':'4g','shuffle_partitions':8},
            'review_code_sha256':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in
                [Path(__file__),Path('src/b_analysis/adapters.py'),Path('src/b_analysis/evaluation.py'),Path('src/predict_safe.py')]}})
        print('B review passed',flush=True)
    finally:spark.stop()

if __name__=='__main__':main()
