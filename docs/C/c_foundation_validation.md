# C foundation validation — 2026-10-06

Scope: synthetic label/interface tests and tiny MLlib environment smoke test.
No historical rainfall has been downloaded, no forecasting model trained, and
no scientific performance claim is made.

## Actual local environment

- macOS 15.7.7 (24G720), arm64.
- CPython 3.11.7 in project-local `.venv`.
- PySpark/Spark 3.5.8; repository requirements installed unchanged.
- Eclipse Temurin JDK 17.0.20.1+1, downloaded via official Adoptium binary API.
- Spark master local[2], shuffle partitions 2, timezone Asia/Singapore, UI off.
- `python -m pip check`: no broken requirements.
- JDK resides outside this repository in workspace `.tools/jdk17/Contents/Home`.
  It is not a system-wide installation or a Git artifact.

Local command (other members substitute their own JDK and checkout paths):

```bash
export JAVA_HOME=/Users/xiaochen/Documents/ChatGPT/DSA5208/.tools/jdk17/Contents/Home
export SPARK_LOCAL_IP=127.0.0.1
source .venv/bin/activate
export PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python"
python -m pytest -q
```

Initial suite: **25 passed in 20.92 seconds**. Includes Spark SQL, fitted
LogisticRegression inside Pipeline, saved PipelineModel reload and probability
agreement within 1e-8. All tests run on small artificial DataFrames.
The added unavailable/cross-boundary null-label test passed separately (1 passed
in 4.50 seconds): **26 tests verified in total**.
The environment required execution outside the app sandbox for the JVM gateway.
`compileall` and `git diff --check` also passed.

## Review handoff

- A: review canonical input, time alignment, version/duplicate and quality
  policies; supply real sample for H1 compatibility and inspect boundary tests.
- B: inspect prediction schema and independently recompute synthetic Brier 0.075
  and TN=2/TP=2/FP=0/FN=0; mark fixture as excluded from every report pipeline.
- C next: integrate H1, build historical features with availability constraints,
  inspect training event rates, then fit M0/M1/M2.

No messages were sent to teammates. Their review/acceptance is pending.
