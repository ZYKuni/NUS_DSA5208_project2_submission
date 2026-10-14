# C: frozen experiment protocol

This protocol and `configs/c_model.json` are specified before opening 2024 predictive performance. This is a resource-bounded experiment, not a claim that every observation was used to fit the estimators.

## Target and input contract

For a station and five-minute endpoint `t`, predict `P(sum(rain(t+5), ..., rain(t+30)) > q)`, with **strict** `>` and `q >= 0`. The six future slots must all exist and be valid; otherwise the outcome is unknown, not dry. The interval-end interpretation is an explicit project assumption, not independently proven by official metadata. A's canonical rainfall is Decimal(10,3); labels retain decimal arithmetic and compare integer 0.001-mm units against `floor(q*1000)`. In particular, 0.200 + 0.400 does not exceed 0.6. Floating-point values are used for features, not target summation.

Raw inputs are the eight annual files in A's manifest at commit `21ee7ad`. Reproduce A's unchanged cleaner; C writes separate cleaning summaries. Verify checksums, 48,538,627 clean rows, schema, and a real seven-row training example before generating full labels. Unobserved station-time keys are not fabricated as forecast origins; the resulting evaluation is conditional on observed origins with complete future outcomes.

## Chronology and sampling

| Split | Forecast origins | Role |
|---|---|---|
| Train | 2017–2022 | Fit estimators; M0 empirical exceedance rates |
| Validation | 2023 Jan–Sep | Select regularization/depth and primary family |
| Calibration | 2023 Oct–Dec | Fit isotonic mappings only |
| Test | 2024 | One final frozen evaluation |

An origin is removed from a split if its 30-minute target reaches the next split boundary. Historical readings before a boundary may still be used when available by the prediction time. Never randomly split expanded threshold rows. Label-independent sampling uses `pmod(xxhash64(station_id, timestamp_seconds, 42),10000)`: bucket <20 for training, <50 for the other splits. Thus expected sample fractions are 0.2% and 0.5%; do not rebalance labels. All stations and years remain eligible. M0 uses all eligible training origins. No year or station coverage is guaranteed by random hashing; record actual support.

Training/calibration thresholds are 0, 0.2, 1, 2, 5, 10 mm. Evaluation additionally includes 0.5 and 3 mm as unseen-threshold interpolation probes. These are experimental values, not official warning categories. Repeated thresholds and overlapping windows are correlated; pooled row counts are not independent sample sizes. No confidence interval or significance claim is made.

## Availability-aware features

An observation is usable only if `event_time <= t`, `available_at <= t`, `quality_ok=true`, and `available_at_basis='update_timestamps'`. This is a conservative **final-version availability proxy**: the static archive cannot reconstruct every revision as originally published. Unknown publication times are excluded. No future amount, label, future completeness or update timestamp is fed to the estimator.

Inputs: station one-hot encoding, sine/cosine hour and day-of-year (leap years handled), `log1p(q)`. Additional history: exact lags 5/10/15/30/60 minutes, missing flags, sums/counts/maxima/wet-slot counts for `(t-15,t]`, `(t-30,t]`, `(t-60,t]`, recent change `2*sum15-sum30`, staleness capped at 65 minutes, and no-history flag. Missing numeric values use zero plus explicit availability indicators; a partial sum is a **known-slot sum**, not a claim that missing rain was zero. Current-slot rain is used only when already published by `t`. Coordinates are omitted to avoid assuming current metadata was historically known.

## Models and selection

- M0: threshold-specific empirical training exceedance probability.
- M1: station/calendar/threshold logistic regression; regularization 0.01 or 0.1, maxIter=60.
- M2: M1 plus as-of history, same logistic grid.
- M3: same inputs as M2, Spark GBT maxDepth 3 or 5, maxIter=30, seed=42.

Choose each family's winner and the primary family by equal-weight mean Brier score over the six **training thresholds** on validation. M0 is a diagnostic reference, not a trainable-family candidate. Keep all candidate validation metrics, predictions, and fit/evaluation timings. No class weighting or oversampling.

Fit one increasing, pooled isotonic mapping per family winner on calibration rows at the six training thresholds. Save raw and calibrated probabilities. Calibration-fit metrics are in-sample diagnostics only. Use calibrated output as the predefined final output; do not switch to raw after viewing test. Alert cutoff is fixed at 0.5 and is not an application-optimized decision policy. No monotonic repair is applied; report adjacent-threshold probability increases, including their maximum size.

## Evaluation, freeze and inference

For each threshold, report n/events/rate, Brier, clipped log loss, exact Spark PR-AUC and ROC-AUC (null if one class), and confusion/precision/recall/F1 at p>=0.5. Also save ten-bin reliability summaries, monthly and station Brier/support, monotonicity checks, and bounded worst-error examples. Spark's trapezoidal PR-AUC is not average precision.

After calibration, `frozen_manifest.json` hashes model files, configs, Task 2 source, common/cleaning source, data manifest and sampled prepared feature files. `test` rejects any changed hash or an already-opened test. The test-start marker is written before reading predictive test metrics. Do not use test results to retune. An interrupted evaluation requires an explicitly recorded recovery decision; deleting the marker to hide a repeated run is prohibited.

The inference CLI loads persisted Spark models and uses the same feature builder. With no usable recent history it explicitly switches to the M1 calendar model; this is a serving safeguard, **not** the family-specific test score. Also evaluate this predefined composite rule separately as `test_serving.json`; its prediction table preserves the actually used family model ID. Unseen stations are encoded as unknown, with a warning. q>10 mm is extrapolation and flagged; arbitrary thresholds are accepted but probability monotonicity is not guaranteed. Prediction time must be aligned to a five-minute grid; naive input means Singapore time.

## Reproduce

From repository root, with Python 3.11, JDK 17, dependencies installed and `JAVA_HOME`, `SPARK_LOCAL_IP=127.0.0.1`, `PYSPARK_PYTHON` set:

```bash
mv reports/c_results reports/c_results_reference
python src/download_data.py
python -m src.task2.reproduce_clean
python -m pytest -q
python -m src.task2.run prepare
python -m src.task2.run train
python -m src.task2.run calibrate
python -m src.task2.run test
python -m src.task2.verify_artifacts
python -m src.task2.report
python -m src.task2.package_artifacts
python -m src.task2.predict S50 '2024-01-01 12:00:00' 1
```

Use a fresh **full Git clone** when retraining: prepare reads the provenance manifest from historical commit 21ee7ad. The first command archives the committed completed-run evidence into a new reference directory; preserve its test markers. This is a separately identified reproduction, not permission to reopen/tune the original run. Existing model/feature/prediction writes are rejected. Deployment inference from the ZIP does not require Git history. Download timestamps may differ from A's manifest, but hashes must match. Results live in `reports/c_results`; large local artifacts in `data/features/c_available_v1`, `data/predictions/c_available_v1`, `models/final/c_available_v1` are excluded from Git and must be packaged separately for submission/handoff.
