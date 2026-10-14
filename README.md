# DSA5208 Project 2: Singapore rainfall with Apache Spark

Group: Yikun Zhao (A0353894M), Zhou Jiahao (A0354090J), Chen Zheke (A0356140M).

This repository contains the complete code, trained models and report for DSA5208 Project 2:

- **Task 1** explores within-year and across-year rainfall patterns in the 2017–2024 five-minute rainfall records from [data.gov.sg](https://data.gov.sg/collections/2279/view).
- **Task 2** estimates the probability that a station accumulates **more than q mm** of rain in the next 30 minutes. The inputs are the station, the prediction time and the threshold q. The models are built and evaluated with Spark MLlib.

| Deliverable | Location |
|---|---|
| Report (PDF) | [`reports/DSA5208_Project2_Report.pdf`](reports/DSA5208_Project2_Report.pdf); editable text in `reports/GROUP_REPORT.md` |
| Source code | `src/` (pipeline and models), `scripts/` (runners, figures, report), `configs/`, `tests/` |
| Trained models | `models/final/c_available_v1/` (see [Trained models](#trained-models)) |
| Saved predictions and sampled features | `data/predictions/c_available_v1/`, `data/features/c_available_v1/` |
| Task 1 result tables and figures | `reports/b_results/` |
| Task 2 metrics and independent review | `reports/c_results/`, `reports/b_model_review/` |

The raw annual CSVs (about 7.5 GB) and the full cleaned table are **not** included because of their size. Steps 2–3 below recreate them from the official source, and the download step verifies their SHA-256 checksums against `data/manifest.csv`.

## 1. Environment

The results were produced with the setup below. Other machines work if the versions match.

| Item | Version / setting |
|---|---|
| OS | macOS 15 (Apple Silicon). Linux also works; on Windows use WSL2. |
| Python | CPython 3.11 |
| Java | JDK 17 (set `JAVA_HOME`) |
| Spark | PySpark 3.5.8 from PyPI, run in local mode (`local[*]` / `local[4]`, 4–6 GB driver memory) |
| Memory / disk | 16 GB RAM; about 20 GB of free disk for the raw and cleaned data |

```bash
# from the repository root
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt          # pyspark, pandas, pyarrow, matplotlib, ...
export JAVA_HOME=/path/to/jdk-17                    # macOS: export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
source scripts/b_env.sh                             # sets PYSPARK_PYTHON, PYTHONPATH, SPARK_LOCAL_IP
python -m pytest -q tests                           # unit tests: cleaning, labels, features, models, interface
```

The machines and timings actually used are recorded in `docs/A/environment.md`, `reports/c_results/environment.md` and Section 2 of the report.

## 2. Download the data

```bash
python src/download_data.py --config configs/project.yaml
```

This downloads the eight annual datasets (2017–2024) into `data/raw/` and checks the byte sizes and SHA-256 checksums. If you only want to verify the delivered models (and not start a new experiment), use `--config configs/c_download_preserve_manifest.yaml` instead, which leaves the frozen `data/manifest.csv` untouched.

## 3. Clean the data (Spark)

```bash
python src/data_quality.py --config configs/project.yaml --stage raw     # raw audit (row counts, time phase, units)
python src/clean_data.py   --config configs/project.yaml                 # -> data/clean/clean_rainfall (Parquet)
python src/data_quality.py --config configs/project.yaml --stage clean   # coverage, gaps, update latency
```

Cleaning keeps 48,538,627 unique station/time records. Missing readings are never replaced by zero. The rules are described in `docs/A/data_dictionary.md`, `docs/A/time_semantics.md` and `docs/A/decisions.md`, and the audit tables are written to `reports/tables/`.

## 4. Task 1: rainfall patterns

```bash
bash scripts/run_b_real.sh reports/b_reproduction    # choose a directory that does not exist yet
```

This runs the Spark aggregations (`src/analysis.py`, `src/analyze_windows.py`, `src/review_extremes.py`), draws the figures, and writes the tables and figures used in the report into the new directory. The submitted results are in `reports/b_results/`.

## 5. Task 2: rainfall exceedance model

### Trained models

`models/final/c_available_v1/` contains Spark `PipelineModel` directories that can be loaded with `pyspark.ml.PipelineModel.load(...)`:

| Model | Description |
|---|---|
| `M1_0.01`, `M1_0.1` | Logistic regression on station and calendar features (regularisation 0.01 / 0.1) |
| `M2_0.01`, `M2_0.1` | Logistic regression with recent-rainfall history features added |
| `M3_3`, `M3_5` | Gradient-boosted trees on the M2 features (depth 3 / 5) |
| `*_calibrator` | Isotonic calibrators for the family winners `M1_0.01`, `M2_0.01`, `M3_5` |
| `selection.json`, `baseline.json` | Validation-based model selection (winner: `M3_5`) and the empirical baseline M0 |

The serving system uses calibrated `M3_5` when recent history is available and calibrated `M1_0.01` otherwise. Its mean Brier score on the 2024 test sample is 0.019248, against 0.027094 for the empirical baseline.

### Recompute the reported metrics from the saved predictions (no raw data needed)

```bash
python -m scripts.review_b_models        # recomputes every validation/test metric -> reports/b_model_review/
python -m scripts.plot_b_model_review    # redraws the model figures
```

### Quick demo: load the saved models and predict (no raw data needed)

```bash
python -m scripts.demo_predict_from_features --list 5                     # some available 2024 test origins
python -m scripts.demo_predict_from_features S06 '2024-11-07 20:50:00' 1  # station, SGT time, q in mm
```

This loads the saved `PipelineModel` and calibrator, applies them to the prediction-time features stored in `data/features/c_available_v1/prepared` for that origin, and prints the probability that the next 30 minutes exceed q. It also prints the rainfall actually observed and the difference from the saved serving prediction, which is 0 for test origins. It uses the same model choice and monotone threshold curve as the full interface below, but it only covers the 134,674 sampled origins in the feature table (use `--split validation`, `calibration` or `train` for other years).

### Predict for any station, time and threshold

```bash
python -m src.predict_safe S50 '2023-01-01 12:00:00' 1
python -m src.predict_safe --help
```

The three arguments are the station id, the prediction time (Asia/Singapore time, on a five-minute boundary, 2017–2024) and the threshold q in mm (0 ≤ q ≤ 10). The output is JSON whose `probability` field is P(rain in the next 30 minutes > q). The features use the station's readings published before the prediction time, so this command needs the cleaned history from step 3 (`data/clean/clean_rainfall`). The interface rules are described in `docs/C/inference_v2.md`.

### Retrain from scratch

```bash
mv reports/c_results reports/c_results_reference    # keep the submitted evidence
python -m src.task2.reproduce_clean
python -m src.task2.run prepare      # labels, available-at-time features, chronological splits, sampling
python -m src.task2.run train        # fit M0-M3 on 2017-2022, select on Jan-Sep 2023
python -m src.task2.run calibrate    # isotonic calibration on Oct-Dec 2023, then freeze hashes
python -m src.task2.run test         # one-time evaluation on 2024
python -m src.task2.report
```

Retraining is not needed to use or check the submitted models; the steps above cover that. Retraining from scratch must be run in a clone of the team's development repository: as part of its integrity checks, `prepare` reads the original download manifest from commit `21ee7ad` of that repository's history (`git show 21ee7ad:data/manifest.csv`), and this submission repository does not contain that commit. The pipeline code is left unchanged so that its hashes still match the frozen-experiment record in `reports/c_results/frozen_manifest.json`. Run it in a fresh checkout so that the submitted models in `models/final/c_available_v1/` are not overwritten. The protocol is described in `docs/C/model_protocol.md`.

## 6. Rebuild the report

```bash
python -m pip install -r requirements-report.txt
python -m scripts.build_group_report     # -> reports/DSA5208_Project2_Report.pdf and reports/GROUP_REPORT.md
```

## Repository layout

```text
configs/   YAML/JSON configuration for cleaning, Task 1 and Task 2
src/       download, quality audit, cleaning, Task 1 analysis (src/analysis.py, src/b_analysis/),
           Task 2 labels/features/models/evaluation (src/task2/), prediction interface (src/predict_safe.py)
scripts/   runners, figure scripts, report builder
tests/     pytest suite
models/    trained Spark pipelines and calibrators
data/      manifest of the source files, small samples, sampled features and saved predictions
reports/   PDF report, result tables, figures and metric files
docs/      methods and decisions (A/, C/); docs/internal/ holds development notes in Chinese
```

## AI usage

AI tools assisted with code, experiments, tests, figures, report drafting, and the final review and revision of the report. The declaration is also in Section 13 of the report.
