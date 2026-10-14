#!/usr/bin/env bash
# Source from repository root. Prefer an already configured JDK 17.
if [ -z "${JAVA_HOME:-}" ]; then
  for candidate in "$PWD"/.runtime/jdk-17*/Contents/Home; do
    if [ -x "$candidate/bin/java" ]; then export JAVA_HOME="$candidate"; break; fi
  done
fi
if [ -z "${JAVA_HOME:-}" ]; then
  echo 'Set JAVA_HOME to a JDK 17 installation.' >&2
  return 1
fi
export PATH="$PWD/.venv/bin:$JAVA_HOME/bin:$PATH"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1
export MPLCONFIGDIR="$PWD/.runtime/matplotlib"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
