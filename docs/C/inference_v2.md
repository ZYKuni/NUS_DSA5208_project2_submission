# A review fixes — guarded inference v2

Use **`python -m src.predict_safe STATION TIME Q`**. Python callers use `src.predict_safe.predict(spark, station_id, prediction_time, threshold_mm, ...)`. The frozen `src.task2.predict` command and c-models-v1 release remain **legacy reproduction only**; they intentionally retain their original behavior and hashes. Updating Git alone does not modify a previously downloaded v1 ZIP.

## Five review findings and v2 behavior

| A's finding | v2 action |
|---|---|
| 2025/out-of-data time silently falls back | Reject dates outside 2017–2024 SGT before starting Spark. Also reject times outside the chosen station's actual first/last observation and unknown stations. No probability is returned on these errors. |
| Non-five-minute time errors without rounding | Retain strict rejection and explicitly document it in CLI help/error/README. No floor/ceiling: silently moving the origin would change available information and the target window. Seconds and microseconds must be zero. |
| Probability increases as threshold increases | Project the six fixed calibrated knot probabilities onto a non-increasing sequence with equal-weight PAVA, then linearly interpolate in rainfall mm. Independent calls use the same fixed knots, never a query-dependent grid. |
| q>10 produces a flat tail/zero | Reject q>10 rather than clamp it to 10 or invent an unsupported tail distribution. For q in [0,10], exact 0/1 estimates remain possible; return BOUNDARY_ESTIMATE and print a warning, not false certainty or an arbitrary epsilon. |
| Naive time timezone undocumented in help | Help states Asia/Singapore (UTC+08:00). Explicit offsets are converted to SGT before range/alignment checks. Output time always includes +08:00. |

Within a station's observed span, a missing/unpublished recent hour may still trigger the M1 fallback. This now produces **CALENDAR_FALLBACK** on stderr and in JSON warnings, with fallback_reason and model_id. An existing station absent from fitting emits UNSEEN_TRAINING_STATION. Station coverage min/max is an archive guard, not evidence that every slot between them exists. The availability-aware feature builder still determines usable history. This is an offline archive interface, not a future live-weather service.

## Examples

```bash
python -m src.predict_safe --help
python -m src.predict_safe S50 '2023-01-01 12:00:00' 1
python -m src.predict_safe S50 '2023-01-01T04:00:00+00:00' 1
# These fail clearly, with exit code 2 and no probability:
python -m src.predict_safe S50 '2025-01-01 12:00:00' 1
python -m src.predict_safe S50 '2024-01-01 12:01:00' 1
python -m src.predict_safe S50 '2024-01-01 12:00:00' 11
```

The two valid time representations denote the same SGT instant; the station must have historical coverage there. Deploy the original saved pipelines/calibrators and clean-history store in the same layout. No model retraining is required.

## Monotonic projection and output semantics

Fixed knots: **[0, 0.2, 1, 2, 5, 10] mm**, the original training thresholds. At each station/time, select the primary/fallback model without reference to q, and evaluate its calibrated predictions at all six knots. Pool adjacent upward violations using equal-weight least-squares isotonic projection in decreasing order. Interpolate the corrected curve linearly in q (mm) between knots. This guarantees non-increasing output for arbitrary q in [0,10] with fixed history/model and consistent station/time, not just within a batch. Flat sections are allowed; strict decrease is neither required nor claimed.

The method reads no labels, fits no extra trainable parameters and adds no extrapolation rule. It is a postprocessing policy, not a newly calibrated distribution. The fixed six-knot grid intentionally does not include the requested q; including it would make results depend on what other thresholds were requested. A synthetic 0.20→0.42 upward jump is pooled to 0.31→0.31. Original raw/calibrated probabilities at the requested q remain in diagnostic fields; **only `probability` is the v2 monotone output**.

New JSON fields include interface_version=c_safe_v2, curve_thresholds_mm, curve_probabilities, unadjusted_calibrated_probability, warnings, fallback_reason, boundary_probability, supported_threshold_mm and evaluation_status. monotonicity_guaranteed applies to the corrected `probability`, not the diagnostic unadjusted fields. Do not interchange v1/v2 outputs when recomputing metrics.

## Evaluation integrity

This change follows review of the already-tested v1 interface. **The v1 test Brier 0.019248 and its other metrics do not evaluate v2.** Model weights/calibrators, original source, prepared features, freeze manifest, test predictions and metrics remain unchanged. No 2024 rerun, hidden refit or claim of untouched-test v2 performance is made.

`tests/test_c_safe_inference.py` covers guards, timezone/help, fallback/boundary warnings and randomized monotonicity checks. `python -m src.verify_safe` checks real S50 predictions selected solely for their largest raw threshold increase in the 2023 validation split (not by labels), makes separate requests at eight thresholds and verifies their common corrected curve. Evidence is in `reports/c_results/safe_v2/acceptance.json`. These are correctness checks, not fresh holdout accuracy estimates. B must label any later v2 metric study separately and explicitly disclose that the original holdout was previously opened.

## Delivery

v1 remains at [c-models-v1](https://github.com/ZYKuni/NUS_DSA5208_project2/releases/tag/c-models-v1) for audit. The separate [v2 release](https://github.com/ZYKuni/NUS_DSA5208_project2/releases/tag/c-models-v2) contains the same weights plus the guarded interface, tests and acceptance evidence. Download its named c_handoff_v2.zip and .sha256 assets and run `shasum -a 256 -c c_handoff_v2.zip.sha256`. `python -m src.package_safe` builds a new local ZIP only when original frozen hashes and v2 acceptance pass; it does not overwrite v1 or upload automatically. Source hashes and postprocessing policy are captured in safe_v2/interface_manifest.json. Never relabel v1 scores as v2. Code/docs remain reviewable through C's PR.
