"""B's independent review case, now a passing regression after C's decimal fix."""
from datetime import datetime,timedelta
from decimal import Decimal
from src.task2.labels import build_future_30m_base,assign_temporal_split,expand_thresholds

def test_c_decimal_equality_boundary(spark):
    start=datetime(2020,1,1)
    rows=[('S',start+timedelta(minutes=5*i),Decimal(v),True)
          for i,v in enumerate(['0','0.2','0.4','0','0','0','0'])]
    frame=spark.createDataFrame(rows,'station_id string,event_time timestamp,rainfall_mm decimal(10,3),quality_ok boolean')
    result=expand_thresholds(assign_temporal_split(build_future_30m_base(frame)),[.6])
    row=result.orderBy('prediction_time').first()
    assert row.label==0., f'Exact 0.600 equals q=0.6, got future={row.future_30m_mm!r}, label={row.label}'
