"""Reuse A's unchanged cleaning CLI with C-local resources and audit outputs."""
import json
import subprocess
import sys
from pathlib import Path
from src.common import load_config


if __name__ == "__main__":
    cfg = load_config("configs/project.yaml")
    cfg["paths"]["tables_dir"] = "reports/c_results/cleaning"
    cfg["spark"].update(master="local[4]", driver_memory="4g", shuffle_partitions=32)
    Path("logs").mkdir(exist_ok=True)
    config_path = Path("logs/c_clean_config.json")
    config_path.write_text(json.dumps(cfg, indent=2))
    subprocess.run([sys.executable, "src/clean_data.py", "--config", str(config_path)], check=True)
