"""Independent reconciliation with A's committed aggregates; bounded tables only."""
from pathlib import Path
import argparse,hashlib,json,subprocess
import pandas as pd
import pyarrow.dataset as ds
from b_analysis.export import spark_arrow_to_pandas

p=argparse.ArgumentParser();p.add_argument('output');root=Path(p.parse_args().output)
csv_dir=root/'csv';csv_dir.mkdir(exist_ok=True)
for kind in ['tables','windows','extremes']:
    for table in (root/kind).iterdir():
        if not table.is_dir():continue
        data=ds.dataset(table,format='parquet')
        if data.count_rows()<=20000:
            spark_arrow_to_pandas(data.to_table()).to_csv(csv_dir/(table.name+'.csv'),index=False)
m=pd.read_csv(csv_dir/'station_month.csv')
a=pd.read_csv('reports/tables/station_month_coverage.csv')
joined=m.merge(a,on=['station_id','year','month'],validate='one_to_one')
assert len(joined)==len(m)==len(a)==91*96
assert (joined.valid_slots==joined.n_valid).all()
assert (joined.wet_slots==joined.n_wet).all()
assert ((joined.coverage-joined.coverage_calendar).abs()<1e-12).all()
assert int(m.valid_slots.sum())==48_538_627
s=pd.read_csv(csv_dir/'sensitivity.csv');assert s.station_count.eq(0).all()
y=pd.read_csv(csv_dir/'overview_year.csv');a_y=pd.read_csv('reports/tables/clean_overview_by_year.csv')
y=y.merge(a_y,on='year',validate='one_to_one')
assert (y.valid_slots==y.clean_rows).all()
assert (y.station_count==y.n_stations).all()
status=pd.read_csv(csv_dir/'window_status.csv')
assert int(status['count'].sum())==48_538_627
old=pd.read_csv('data/manifest.csv').set_index('year');new=pd.read_csv('data/b_manifest.csv').set_index('year')
assert old.sha256.equals(new.sha256)
record={'upstream_a':'d90f400','upstream_c':'60169d1',
    'git_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
    'matched_month_rows':len(joined),'matched_clean_rows':int(m.valid_slots.sum()),
    'raw_sha256_match':True,
    'source_sha256':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in Path('src').rglob('*.py')},
    'script_sha256':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in Path('scripts').glob('*') if p.is_file()},
    'config_sha256':hashlib.sha256(Path('configs/b_real.yaml').read_bytes()).hexdigest()}
(root/'validation.json').write_text(json.dumps(record,indent=2))
print('PASS: eight file hashes, 8736 station-month counts/coverage, annual overview, 48538627 anchors reconciled.')
