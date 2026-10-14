"""B reviews C's top windows against A's exact-decimal observations, no new labels."""
import argparse
from pathlib import Path
from pyspark.sql import functions as F, Window
from b_analysis.common import session,write_tables

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--clean',required=True);p.add_argument('--windows',required=True)
    p.add_argument('--output',required=True);args=p.parse_args();spark=session()
    try:
        clean=spark.read.parquet(args.clean)
        top=spark.read.parquet(str(Path(args.windows)/'top_30m_candidates'))
        # A station/day is a selection unit, not a declaration of independent storms.
        w=Window.partitionBy('station_id',F.to_date('prediction_time')).orderBy(F.desc('future_30m_mm'),'prediction_time')
        cases=(top.withColumn('_rank',F.row_number().over(w)).where('_rank=1')
               .orderBy(F.desc('future_30m_mm'),'station_id','prediction_time').limit(3)
               .select(F.col('station_id').alias('case_station'),F.col('prediction_time').alias('anchor'),
                       F.col('future_30m_mm').alias('c_future_30m_mm')))
        anchor_obs=clean.select(F.col('station_id').alias('case_station'),F.col('timestamp_sgt').alias('anchor'),
                                F.col('latitude').alias('case_lat'),F.col('longitude').alias('case_lon'))
        cases=cases.join(anchor_obs,['case_station','anchor']).withColumn('case_id',
            F.concat_ws('_','case_station',F.date_format('anchor','yyyyMMdd_HHmm')))
        obs=clean.select(F.col('station_id').alias('observed_station'),'timestamp_sgt','rainfall_mm','latitude','longitude')
        contexts=(obs.join(F.broadcast(cases),
             (F.col('timestamp_sgt')>=F.col('anchor')-F.expr('INTERVAL 60 MINUTES')) &
             (F.col('timestamp_sgt')<=F.col('anchor')+F.expr('INTERVAL 30 MINUTES')) &
             (F.abs(F.col('latitude')-F.col('case_lat'))<=.06) &
             (F.abs(F.col('longitude')-F.col('case_lon'))<=.06))
             .withColumn('relative_minutes',(F.unix_timestamp('timestamp_sgt')-F.unix_timestamp('anchor'))/60)
             .withColumn('window_role',F.when(F.col('relative_minutes')>0,'future_observed').otherwise('past_observed')))
        target=contexts.where((F.col('observed_station')==F.col('case_station'))&(F.col('relative_minutes')>0))
        check=(target.groupBy('case_id','case_station','anchor','c_future_30m_mm')
              .agg(F.count('*').alias('future_slots'),F.sum('rainfall_mm').alias('decimal_future_mm'))
              .withColumn('double_difference',F.col('decimal_future_mm').cast('double')-F.col('c_future_30m_mm')))
        if check.where((F.col('future_slots')!=6)|(F.abs('double_difference')>1e-9)).count():
            raise ValueError('Candidate window disagrees with exact six-observation arithmetic')
        write_tables({'case_context':contexts,'case_window_check':check},args.output)
    finally:spark.stop()
