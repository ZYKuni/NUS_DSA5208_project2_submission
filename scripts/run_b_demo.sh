#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/b_env.sh
out="${1:-reports/demo_b}"
if [ -e "$out" ]; then echo "Output exists: $out. Choose a new path to preserve provenance." >&2; exit 1; fi
mkdir -p "$out"
python src/make_b_fixtures.py --output "$out/fixtures"
spark-submit --master 'local[2]' --conf spark.sql.shuffle.partitions=2 src/analysis.py \
  --config configs/b_demo.yaml --clean "$out/fixtures/clean.csv" --stations "$out/fixtures/stations.csv" \
  --output "$out/eda_tables" --fixture
spark-submit --master 'local[2]' --conf spark.sql.shuffle.partitions=2 src/verify_predictions.py \
  --config configs/b_demo.yaml --predictions "$out/fixtures/predictions.csv" \
  --output "$out/evaluation_tables" --fixture
python src/plots.py --tables "$out/eda_tables" --output "$out/eda_figures"
python src/plots.py --tables "$out/evaluation_tables" --output "$out/evaluation_figures"
python scripts/check_b_demo.py "$out"
printf 'Demo complete: %s (SYNTHETIC ONLY)\n' "$out"
