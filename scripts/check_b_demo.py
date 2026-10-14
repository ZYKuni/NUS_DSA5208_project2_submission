"""Check known synthetic end-to-end results, never use on real datasets."""
import argparse
import math
from pathlib import Path
import pyarrow.dataset as ds

p=argparse.ArgumentParser()
p.add_argument('output',nargs='?',default='reports/demo_b')
root=Path(p.parse_args().output)
def read(kind,name):
    table=ds.dataset(root/kind/name,format='parquet')
    if table.count_rows()>20000:
        raise ValueError('Unexpected fixture size')
    return table.to_table().to_pandas()
s=read('eda_tables','sensitivity').set_index('coverage_threshold')
assert s.station_count.to_dict()=={.9:3,.95:2,.98:2}
m=read('eda_tables','station_month')
assert m.valid_slots.sum()==52080
assert m.loc[m.station_id=='DEMO_ABSENT','observed_rainfall_mm'].isna().all()
assert (m.loc[m.station_id=='DEMO_ABSENT','coverage']==0).all()
assert m.loc[m.station_id=='DEMO_DRY','wet_mean_mm'].isna().all()
e=read('evaluation_tables','metrics')
r=e[(e.model_name=='demo_raw') & (e.threshold_mm==0)].iloc[0]
assert math.isclose(r.brier,.095,abs_tol=1e-12)
assert (r.tp,r.fp,r.tn,r.fn)==(4,0,4,0)
print('Synthetic end-to-end checks passed: missing/dry, fixed stations, Brier, confusion matrix.')
