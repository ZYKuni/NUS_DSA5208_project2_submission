"""Run from repository root: spark-submit src/analysis.py --help."""
import argparse
from b_analysis.common import (load_config, session, read_table, CLEAN, STATIONS,
                               write_tables, manifest, check_mode)
from b_analysis.eda import analyze

if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Member B Spark EDA; cleaned data only')
    p.add_argument('--config', required=True)
    p.add_argument('--clean', required=True)
    p.add_argument('--stations', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--fixture', action='store_true')
    p.add_argument('--input-contract', choices=['b','a'], default='b')
    args = p.parse_args()
    cfg = load_config(args.config)
    check_mode(cfg, args.fixture)
    spark = session()
    try:
        if args.input_contract == 'a':
            from b_analysis.adapters import adapt_a, read_named
            clean, stations = adapt_a(read_named(spark,args.clean),read_named(spark,args.stations))
        else:
            clean, stations = read_table(spark,args.clean,CLEAN),read_table(spark,args.stations,STATIONS)
        tables = analyze(spark, clean, stations, cfg)
        write_tables(tables, args.output)
        manifest(args.output, cfg, {'clean': args.clean, 'stations': args.stations}, 'eda', args.fixture)
    finally:
        spark.stop()
