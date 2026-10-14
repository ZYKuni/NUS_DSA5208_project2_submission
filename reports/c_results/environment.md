# C actual execution environment

- Date: 2026-10-06 (Asia/Singapore).
- Local Apple M2 Pro, 10 CPU cores, 16 GiB physical RAM; macOS 15.7.7 arm64.
- CPython 3.11.7 in repository `.venv`; PySpark 3.5.8; Temurin JDK 17.0.20.1.
- Main ETL/training/evaluation: Spark `local[4]`, driver memory 4g, 32 shuffle partitions. This is one local machine, not four distributed machines. Default AQE may coalesce partitions.
- Inference/reload verification: `local[2]`, driver 2g, 4 shuffle partitions. Tests use their fixture configuration.
- Session and driver time zone: Asia/Singapore. `SPARK_LOCAL_IP=127.0.0.1`; Python worker explicitly uses project `.venv/bin/python`.
- 8,047,044,701 bytes of raw CSV, downloaded to local SSD. Large input/features/predictions/models are ignored by Git, not absent from the actual experiment.
- Frozen model manifest records actual Python/Spark/Java versions and hashes. Per-stage timing JSON records Spark configuration and elapsed seconds. Candidate experiment timings include fitting plus validation evaluation (not pure training).
- 37 tests passed in 38.42 seconds before real training. A's cleaner is reused unmodified, with C-local resource and audit-output configuration.
- Final regression rerun after model delivery checks: 37 passed in 24.25 seconds. This only reran unit/integration tests, not model fitting or the frozen 2024 evaluation.
- After integrating B's newly merged main (`5b019fc`) and removing the now-obsolete strict xfail marker: **56 passed, 1 dependency deprecation warning in 42.72 seconds**. No frozen training/inference source, configuration, model or prepared feature was changed by the merge.
- Full clean run: 21:22:55–21:33:10 SGT (about 615 seconds); 48,538,627 rows, zero duplicate station/time keys. C's `cleaning/cleaning_log.csv` is byte-for-byte identical to A's committed `reports/tables/cleaning_log.csv`. Spark released its temporary shuffle storage on successful completion.
- `pip_freeze.txt` captures the actual installed dependency set; `requirements.txt` is the team's direct-dependency contract.

Resource limitation: only 0.2% of eligible training origins and 0.5% of other split origins are selected by a fixed, outcome-independent hash; full data are nevertheless cleaned and labeled, and M0 uses every eligible training origin. Sampling retains the natural event distribution in expectation, but high-threshold/individual-station estimates may be noisy.
