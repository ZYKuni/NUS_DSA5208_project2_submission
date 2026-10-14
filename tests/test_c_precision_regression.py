from datetime import datetime, timedelta
from decimal import Decimal
from src.task2.labels import build_future_30m_base, assign_temporal_split, expand_thresholds


def test_c_decimal_equality_boundary(spark):
    start = datetime(2020, 1, 1)
    rows = [('S', start+timedelta(minutes=5*i), Decimal(v), True)
            for i, v in enumerate(['0', '0.2', '0.4', '0', '0', '0', '0'])]
    frame = spark.createDataFrame(rows, 'station_id string,event_time timestamp,rainfall_mm decimal(10,3),quality_ok boolean')
    result = expand_thresholds(assign_temporal_split(build_future_30m_base(frame)),
                               [.5999999999999999, .6, .6000000000000001, 1e100])
    rows = result.where(result.prediction_time == start).orderBy('threshold_mm').collect()
    assert [r.label for r in rows] == [1., 0., 0., 0.]
    assert all(r.future_30m_mm == Decimal('.600') for r in rows)
