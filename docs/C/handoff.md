# C handoff / team review checklist

**A review update:** use the guarded **v2** entry point `python -m src.predict_safe`; see [inference_v2.md](inference_v2.md) for the five fixes and new output semantics. Frozen `src.task2.predict` is legacy v1 reproduction only. v2 uses the same weights but changes probabilities through monotone postprocessing, so v1 holdout scores must not be quoted as v2 performance. Use `python -m src.verify_safe` for v2 acceptance checks on 2023 validation; the original verify_artifacts command checks v1 only.

## What the implementation delivers

1. Experiments based on main `21ee7ad` (including A's actual pipeline and C's foundation); subsequently integrated main `5b019fc`, which adds B's completed analysis without changing C's frozen source/models/features.
2. Decimal threshold bug fixed; permanent non-xfail regression in `tests/test_c_precision_regression.py`.
3. A's eight files independently downloaded and checksummed; cleaning log reproduced exactly; real S66 target checked in `reports/c_results/h1_acceptance.json`.
4. Exact-slot, publication-aware history features with explicit missing flags and counts.
5. M0, M1 calendar LR and M2 history LR; fixed validation exports and metrics.
6. M3 GBT, held-out-time calibration, immutable freeze gate, 2024 evaluation, persisted models and three-input inference. Completion is evidenced by `test_complete.json` and `reload_verification.json`, not this checklist alone.

See `model_protocol.md`, `reports/c_results/result_tables.md` and the final C results discussion for methodology and actual findings. Raw/clean/feature/model outputs are intentionally outside ordinary Git; the PR is not the model artifact.

## Files for B

- `reports/c_results/validation_*.json`: each candidate's genuine 2023 validation metrics.
- `data/predictions/c_available_v1/validation/*`: actual candidate predictions, not the old synthetic fixture.
- `data/predictions/c_available_v1/test/{M0,<family winner>,serving}`: final frozen 2024 predictions.
- `reports/c_results/test_*.json`: raw and calibrated scores, reliability bins, station/month aggregates and monotonicity checks. Metrics use Spark trapezoidal PR-AUC, not sklearn average precision.
- `docs/C/prediction_schema.md`: v0.2 decimal target schema and added inference_mode.

B should independently recompute Brier and confusion counts on the **full exported sampled prediction set**, grouped by threshold and chosen probability column. Preserve natural event frequencies and report origin counts, not eight-threshold rows as independent cases. For serving predictions, model_id varies with fallback; aggregate all serving rows for the system-level score before optionally stratifying by model_id.

C has also run `python -m src.check_c_handoff`, reusing B's unchanged adapter/metric code on all 201,816 M3 validation prediction rows (8 thresholds). Counts/confusion agree exactly and Brier/log loss/ROC-AUC/PR-AUC agree within 1e-12; evidence is `reports/c_results/b_code_interoperability.json`. This is C-executed cross-implementation verification, not B's human acceptance. B's v0.1 adapter still replaces inference_mode with unspecified_by_C; actual v0.2 predictions contain available_at_proxy, and B should preserve it when updating the report interface.

`feature_complete` is descriptive, not an inclusion filter. It is always false under the strict publication cutoff, while known-slot averages are roughly 9–10 of 12. Use label_available and finite probability as eligibility checks.

B's analysis was merged into main at `5b019fc` while C completed delivery. Its `tests/test_c_precision_review.py` originally marked the old defect `xfail(strict=True)`. C's integration removes that marker, retaining B's unchanged assertion as a normal passing regression. C's additional regression uses a distinct filename. No changes have been pushed to B's branch by C. B's earlier status/review documents are historical records, not current evidence that the repaired defect remains open.

## Files for A

- `models/final/c_available_v1/selection.json`: chosen primary and per-family winners.
- Winner Spark PipelineModel directories and matching `*_calibrator` IsotonicRegressionModel directories; preserve directory structure, metadata and data files.
- `src/task2/predict.py`: `python -m src.task2.predict STATION 'YYYY-MM-DD HH:MM:SS' Q`.
- `reports/c_results/reload_verification.json`: C's new-session batch-versus-interface parity and unknown-station fallback evidence.
- `reports/c_results/environment.md`: actual C machine/configuration; this complements A's environment rather than overwriting it.

A should independently reload the model in the agreed environment and verify the API using reproduced clean history. The actual three user inputs are station, time and threshold; the local clean-history store and model root are deployment configuration, not extra user-facing predictors. An operational live-data fetcher is not included.

## Local artifact and remaining group obligations

After all checks pass, `python -m src.task2.package_artifacts` creates `output/c_handoff_v1.zip`; its SHA-256 and content summary are in `reports/c_results/artifact_manifest.json`. It contains source, configs, docs, small reports/figures, models, prediction tables, and sampled prepared features. Raw/clean data and full-population labels are omitted and reproducible. Zip CRC is verified automatically. The packaging script itself does not upload or submit anything.

The original verified ZIP is now distributed as an asset of [Release c-models-v1](https://github.com/ZYKuni/NUS_DSA5208_project2/releases/tag/c-models-v1), targeting commit c0885c5. Download [the model ZIP](https://github.com/ZYKuni/NUS_DSA5208_project2/releases/download/c-models-v1/c_handoff_v1.zip) and [checksum file](https://github.com/ZYKuni/NUS_DSA5208_project2/releases/download/c-models-v1/c_handoff_v1.zip.sha256); run `shasum -a 256 -c c_handoff_v1.zip.sha256` in their directory. Expected SHA-256: `42b149156ee9e9ec99f203cb4a26f911ad4937852a7fb43731c559a1a04dc3a9`. Use the named model asset, not GitHub's generated source archives. Repository permissions still apply. This is a pre-release pending team acceptance, not a production deployment or Canvas submission. Code/document review remains in PR #3; no automatic merge is performed. New reproductions must not overwrite this frozen asset.

The group still needs to combine B's now-merged Task 1 report with C's contribution, B's independent real-metric review, A's independent model reload, final report assembly with all members' real names/student IDs, AI disclosure, and Canvas submission. C's completed experiments do not substitute for those human/group responsibilities. Do not tune this frozen run on the final test results.
