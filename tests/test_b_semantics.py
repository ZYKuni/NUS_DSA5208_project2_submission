import math
import pytest
from pyspark.sql import functions as F
from b_analysis.common import CLEAN, STATIONS, PREDICTIONS, month_calendar
from b_analysis.eda import summary, fixed_stations, validate as validate_clean
from b_analysis.evaluation import evaluate, validate as validate_predictions


def test_rainfall_denominators_and_dry_conditional(spark):
    frame=spark.createDataFrame([('wet',0.),('wet',2.),('wet',4.),('dry',0.)],
                               'station_id string, rainfall_mm double')
    result={r.station_id:r for r in summary(frame,['station_id']).collect()}
    assert result['wet'].wet_frequency == pytest.approx(2/3)
    assert result['wet'].mean_mm_per_valid_slot == 2
    assert result['wet'].wet_mean_mm == 3
    assert result['dry'].wet_mean_mm is None


def test_leap_year_calendar(spark):
    rows=month_calendar(spark,{'start':'2024-02-01','end':'2024-03-01'}).collect()
    assert rows[0].expected_slots == 29*288


def test_missing_month_and_relocated_station_excluded(spark):
    months=spark.createDataFrame([('ok',.96),('ok',.96),('gap',1.),('gap',0.),('move',1.),('move',1.)],
                                 'station_id string, coverage double')
    stations=spark.createDataFrame([('ok',True),('gap',True),('move',False)],'station_id string, comparable boolean')
    assert [r.station_id for r in fixed_stations(months,stations,.95,2).collect()] == ['ok']
    assert fixed_stations(months,stations,.98,2).count()==0


def clean_rows(spark,rows):
    clean=spark.createDataFrame(rows,CLEAN)
    stations=spark.createDataFrame([('S1',1.3,103.8,True)],STATIONS)
    return validate_clean(clean,stations)


def test_missing_not_zero_and_sgt(spark):
    frame=clean_rows(spark,[('S1','2024-01-01T16:00:00Z',None,'missing'),
                           ('S1','2024-01-02T00:05:00+08:00',0.,'valid')])
    valid=frame.where("quality_flag = 'valid'")
    assert valid.count()==1
    assert frame.select(F.dayofmonth('timestamp_sgt')).first()[0]==2


@pytest.mark.parametrize('rows',[
 [('S1','2024-01-01T00:00:00+08:00',-1.,'valid')],
 [('S1','2024-01-01T00:00:00+08:00',float('nan'),'valid')],
 [('S1','2024-01-01T00:04:59+08:00',0.,'valid')],
 [('S1','2024-01-01T00:00:00+08:00',0.,'valid')]*2,
])
def test_bad_clean_inputs_fail(spark,rows):
    with pytest.raises(ValueError): clean_rows(spark,rows)


def predictions(spark,labels=(0.,0.,1.,1.),probabilities=(0.1,0.4,0.6,0.8)):
    return spark.createDataFrame([('r','m',str(i),'S1',f'2023-01-01T00:{i*5:02d}:00+08:00',
                                   0.,y,p,'validation','calendar')
                                  for i,(y,p) in enumerate(zip(labels,probabilities))],PREDICTIONS)


def test_hand_calculated_brier_confusion_and_bins(spark):
    tables=evaluate(predictions(spark),{'alert_probability':.5,'reliability_bins':10})
    row=tables['metrics'].first()
    assert row.n==4 and row.positive_count==2
    assert row.brier==pytest.approx(.0925)
    assert row.log_loss==pytest.approx(-(math.log(.9)+math.log(.6)+math.log(.6)+math.log(.8))/4)
    assert (row.tp,row.fp,row.tn,row.fn)==(2,0,2,0)
    assert row.roc_auc==pytest.approx(1.)
    assert row.pr_auc==pytest.approx(1.)
    assert tables['reliability'].agg(F.sum('n')).first()[0]==4


def test_single_class_auc_null_and_probability_one_in_last_bin(spark):
    tables=evaluate(predictions(spark,(0.,0.),(0.,1.)),{'alert_probability':.5,'reliability_bins':10})
    row=tables['metrics'].first()
    assert row.roc_auc is None and row.pr_auc is None and row.recall is None
    assert math.isfinite(row.log_loss)
    assert {r.bin for r in tables['reliability'].collect()}=={0,9}


def test_duplicate_predictions_and_split_leak_fail(spark):
    frame=predictions(spark)
    with pytest.raises(ValueError): validate_predictions(frame.union(frame))
    leaked=frame.withColumn('model_name',F.lit('other')).withColumn('split',F.lit('test'))
    with pytest.raises(ValueError): validate_predictions(frame.union(leaked))


def test_out_of_range_probability_fails(spark):
    with pytest.raises(ValueError): validate_predictions(predictions(spark, (0.,),(1.1,)))


def test_duplicate_physical_origin_cannot_hide_behind_new_ids(spark):
    frame=predictions(spark)
    renamed=frame.withColumn('base_sample_id',F.concat(F.lit('other_'),F.col('base_sample_id')))
    with pytest.raises(ValueError): validate_predictions(frame.union(renamed))


def test_reference_selection_uses_every_year_and_location_stability(spark):
    from b_analysis.eda import reference_stations
    years=spark.createDataFrame([('ok',.8),('ok',.9),('gap',.79),('gap',1.),('move',1.),('move',1.)],
                               'station_id string, coverage double')
    stations=spark.createDataFrame([('ok',True),('gap',True),('move',False)],'station_id string, comparable boolean')
    assert [r.station_id for r in reference_stations(years,stations,.8,2).collect()]==['ok']
