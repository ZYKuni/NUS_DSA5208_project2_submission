"""B's independent verification, separate from C's evaluate.py."""
import argparse
from b_analysis.common import (load_config, session, read_table, PREDICTIONS,
                               write_tables, manifest, check_mode)
from b_analysis.evaluation import evaluate

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--config', required=True)
    p.add_argument('--predictions', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--fixture', action='store_true')
    p.add_argument('--input-contract', choices=['b','c'], default='b')
    args = p.parse_args()
    cfg = load_config(args.config)
    check_mode(cfg, args.fixture)
    spark = session()
    try:
        if args.input_contract == 'c':
            from b_analysis.adapters import adapt_c, read_named
            pred, audit = adapt_c(read_named(spark,args.predictions),args.fixture)
            tables = evaluate(pred,cfg)
            tables['input_audit'] = audit
        else:
            tables = evaluate(read_table(spark,args.predictions,PREDICTIONS),cfg)
        write_tables(tables,args.output)
        manifest(args.output, cfg, {'predictions': args.predictions}, 'evaluation', args.fixture)
    finally:
        spark.stop()
