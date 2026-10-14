#!/usr/bin/env bash
# Run after A's clean_data.py has produced data/clean/clean_rainfall and stations.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/b_env.sh
out="${1:-reports/b_results}"
if [ -e "$out" ]; then echo "Choose a new output directory: $out already exists" >&2; exit 1; fi
mkdir -p "$out"
start=$(date +%s)
spark-submit --master 'local[4]' --driver-memory 6g --conf spark.sql.shuffle.partitions=32 \
  src/analysis.py --input-contract a --config configs/b_real.yaml \
  --clean data/clean/clean_rainfall --stations data/clean/stations --output "$out/tables"
python src/plots.py --tables "$out/tables" --output "$out/figures"
spark-submit --master 'local[4]' --driver-memory 6g --conf spark.sql.shuffle.partitions=32 \
  src/analyze_windows.py --config configs/b_real.yaml --clean data/clean/clean_rainfall --output "$out/windows"
spark-submit --master 'local[4]' --driver-memory 4g --conf spark.sql.shuffle.partitions=16 \
  src/review_extremes.py --clean data/clean/clean_rainfall --windows "$out/windows" --output "$out/extremes"
python scripts/verify_b_real.py "$out"
python scripts/plot_b_supplements.py "$out"
python scripts/write_b_report.py "$out"
printf 'B analysis elapsed seconds: %s\n' "$(( $(date +%s) - start ))" > "$out/timing.txt"
