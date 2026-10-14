"""Record the actual computing environment for the report (P0-02 / 10.3).

    python src/record_environment.py --config configs/project.yaml

Writes logs/environment.json and logs/pip_freeze.txt, and prints a Markdown
table to paste into docs/A/environment.md.
"""
from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import sys

from common import REPO_ROOT, base_arg_parser, get_spark, load_config, repo_path


def _run(cmd: list[str]) -> str:
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return (out.stdout + out.stderr).strip()
    except (OSError, subprocess.SubprocessError) as exc:
        return f"unavailable: {exc}"


def hardware() -> dict:
    info = {"machine": platform.machine(), "logical_cpus": os.cpu_count()}
    if sys.platform == "darwin":
        info["cpu"] = _run(["sysctl", "-n", "machdep.cpu.brand_string"])
        info["memory_bytes"] = int(_run(["sysctl", "-n", "hw.memsize"]) or 0)
        info["model"] = _run(["sysctl", "-n", "hw.model"])
    elif os.path.exists("/proc/meminfo"):
        with open("/proc/meminfo") as fh:
            kb = int(fh.readline().split()[1])
        info["memory_bytes"] = kb * 1024
        info["cpu"] = _run(["sh", "-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2"])
    total, _, free = shutil.disk_usage(REPO_ROOT)
    info["disk_total_bytes"], info["disk_free_bytes"] = total, free
    return info


def main() -> int:
    args = base_arg_parser(__doc__).parse_args()
    cfg = load_config(args.config)
    spark = get_spark(cfg, "dsa5208-environment")
    jvm = spark.sparkContext._jvm.java.lang.System
    env = {
        "os": platform.platform(),
        "python": sys.version.split()[0],
        "java": f"{jvm.getProperty('java.vendor')} {jvm.getProperty('java.version')}",
        "spark": spark.version,
        "spark_conf": dict(sorted(spark.sparkContext.getConf().getAll())),
        "hardware": hardware(),
    }
    spark.stop()
    logs = repo_path(cfg, "logs_dir")
    logs.mkdir(parents=True, exist_ok=True)
    (logs / "environment.json").write_text(json.dumps(env, indent=2, default=str))
    (logs / "pip_freeze.txt").write_text(_run([sys.executable, "-m", "pip", "freeze"]))

    hw = env["hardware"]
    conf = env["spark_conf"]
    rows = [
        ("平台 / 机器", f"{hw.get('model', '')} {hw['machine']}".strip()),
        ("CPU", f"{hw.get('cpu', '')} ({hw['logical_cpus']} logical)"),
        ("内存", f"{hw.get('memory_bytes', 0) / 2**30:.1f} GiB"),
        ("磁盘（仓库所在卷）", f"{hw['disk_total_bytes'] / 1e9:.0f} GB total, "
                          f"{hw['disk_free_bytes'] / 1e9:.0f} GB free"),
        ("OS", env["os"]),
        ("Python / Java / Spark", f"{env['python']} / {env['java']} / {env['spark']}"),
        ("执行模式", conf.get("spark.master", "")),
        ("Driver 内存", conf.get("spark.driver.memory", "")),
        ("shuffle 分区", conf.get("spark.sql.shuffle.partitions", "")),
        ("时区", conf.get("spark.sql.session.timeZone", "")),
    ]
    print("| 字段 | 实际值 |\n|---|---|")
    for k, v in rows:
        print(f"| {k} | {v} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
