from pathlib import Path
import pytest
from pyspark.sql import functions as F
from b_analysis.adapters import adapt_a,adapt_c,read_named
from b_analysis.evaluation import evaluate

ROOT=Path(__file__).resolve().parents[1]


def test_a_real_sample_consumed_without_recleaning(spark):
    clean,stations=adapt_a(read_named(spark,ROOT/'data/samples/clean_sample.csv'),
                          read_named(spark,ROOT/'reports/tables/station_metadata.csv'))
    assert clean.count()>0
    assert clean.select('quality_flag').distinct().first()[0]=='valid'
    assert stations.where("station_id = 'S113'").first().comparable is False


def test_c_real_contract_fixture_independent_metrics(spark):
    frame=read_named(spark,ROOT/'data/samples/predictions_fixture.csv')
    pred,audit=adapt_c(frame,True)
    assert pred.count()==4  # absent calibrator is not silently replaced by raw p
    assert audit.where("probability_version='calibrated' AND exclusion_reason='probability_unavailable_or_invalid'").first()['count']==4
    row=evaluate(pred,{'alert_probability':.99,'reliability_bins':10})['metrics'].first()
    assert row.brier==pytest.approx(.075)
    assert row.alert_probability==.5  # C supplied cutoff overrides fallback config
    assert (row.tp,row.fp,row.tn,row.fn)==(2,0,2,0)
    with pytest.raises(ValueError): adapt_c(frame,False)


def test_c_missing_label_audited_and_prediction_mismatch_rejected(spark):
    frame=read_named(spark,ROOT/'data/samples/predictions_fixture.csv')
    missing=frame.withColumn('label_available',F.lit(False)).withColumn('label',F.lit(None))
    pred,audit=adapt_c(missing,True)
    assert pred.count()==0
    assert audit.where("exclusion_reason='label_unavailable'").agg(F.sum('count')).first()[0]==8
    with pytest.raises(ValueError): adapt_c(frame.withColumn('prediction',F.lit(99)),True)



def test_spark_arrow_utc_export_is_explicit_sgt():
    import pyarrow as pa
    from datetime import datetime
    from b_analysis.export import spark_arrow_to_pandas
    table=pa.table({'prediction_time':pa.array([datetime(2022,8,21,11,55)],type=pa.timestamp('us'))})
    assert spark_arrow_to_pandas(table).iloc[0].prediction_time.isoformat()=='2022-08-21T19:55:00+08:00'


def test_v02_serving_keeps_mode_and_combines_fallback_without_filtering_history(spark):
    frame=read_named(spark,ROOT/'data/samples/predictions_fixture.csv')
    frame=(frame.withColumn('inference_mode',F.lit('available_at_proxy'))
           .withColumn('feature_complete',F.lit(False))
           .withColumn('model_id',F.when(F.col('label')==1,'history').otherwise('calendar')))
    pred,_=adapt_c(frame,True,system_id='serving_v1')
    assert pred.count()==4
    assert {r[0] for r in pred.select('source_model_id').distinct().collect()}=={'history','calendar'}
    assert pred.select('inference_mode').distinct().first()[0]=='available_at_proxy'
    rows=evaluate(pred,{'alert_probability':.5,'reliability_bins':10})['metrics'].collect()
    assert len(rows)==1 and rows[0].n==4 and rows[0].brier==pytest.approx(.075)
    with pytest.raises(ValueError):
        adapt_c(frame.unionByName(frame.withColumn('model_id',F.lit('duplicate'))),True,system_id='serving_v1')
