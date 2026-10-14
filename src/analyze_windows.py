"""E6 descriptive 30-minute distribution, reusing C's sole window implementation."""
import argparse
from pathlib import Path
from pyspark.sql import functions as F
from pyspark import StorageLevel
from b_analysis.common import session,load_config,write_tables,manifest,check_mode
from task2.labels import build_future_30m_base

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--config',required=True);p.add_argument('--clean',required=True)
    p.add_argument('--output',required=True)
    args=p.parse_args();cfg=load_config(args.config);check_mode(cfg,False)
    spark=session()
    try:
        clean=spark.read.parquet(args.clean)
        base=build_future_30m_base(clean).persist(StorageLevel.DISK_ONLY)
        status=base.groupBy('label_available','label_drop_reason').count()
        good=base.where('label_available').withColumn('year',F.year('prediction_time'))
        distribution=good.groupBy('year').agg(F.count('*').alias('n_complete_windows'),
            F.avg('future_30m_mm').alias('mean_30m_mm'),
            F.expr('percentile_approx(future_30m_mm, array(0.5,0.9,0.99,0.999),10000)').alias('quantiles'),
            F.max('future_30m_mm').alias('max_30m_mm'))
        top=good.orderBy(F.desc('future_30m_mm'),'station_id','prediction_time').limit(30)
        write_tables({'window_status':status,'window_distribution':distribution,'top_30m_candidates':top},args.output)
        manifest(args.output,cfg,{'clean':args.clean,'label_builder':'src/task2/labels.py@60169d1'},'windows',False)
    finally:spark.stop()
