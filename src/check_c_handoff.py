"""C-run interoperability check with B's independent implementation; no fitting.

Run python -m src.check_c_handoff. This is not B's human H2 acceptance.
Only persisted 2023 validation predictions are read; the test is not reopened.
"""
import json
from pathlib import Path
from src.common import get_spark, load_config
from src.b_analysis.adapters import adapt_c
from src.b_analysis.evaluation import evaluate


def main():
    cfg = load_config("configs/project.yaml")
    cfg["spark"].update(master="local[2]", driver_memory="2g", shuffle_partitions=4)
    spark = get_spark(cfg, "C-handoff-independent-code-check")
    try:
        original = spark.read.parquet("data/predictions/c_available_v1/validation/M3_5")
        adapted, audit = adapt_c(original, fixture=False)
        tables = evaluate(adapted, {"reliability_bins": 10, "alert_probability": .5})
        actual = [r.asDict() for r in tables["metrics"].collect()]
        expected = {r["threshold_mm"]: r for r in json.loads(Path("reports/c_results/validation_M3_5.json").read_text())["metrics"]}
        assert len(actual) == len(expected) == 8
        maximum = 0.
        for row in actual:
            ref = expected[row["threshold_mm"]]
            for key in ["n", "positive_count", "tp", "fp", "tn", "fn"]:
                assert row[key] == ref[key], (row["threshold_mm"], key)
            for key in ["brier", "log_loss", "roc_auc", "pr_auc"]:
                difference = abs(row[key]-ref[key])
                maximum = max(maximum, difference)
                assert difference < 1e-12, (row["threshold_mm"], key, difference)
        output = {"passed": True, "executed_by": "C using B's unchanged metric/adapter implementation",
            "not_human_B_acceptance": True, "input": "validation/M3_5", "threshold_groups": 8,
            "rows": sum(r["n"] for r in actual), "max_metric_absolute_difference": maximum,
            "count_and_confusion_match_exactly": True,
            "adapter_note": "B v0.1 adapter labels inference_mode unspecified_by_C; C v0.2 source exports available_at_proxy. Metrics unaffected.",
            "metrics": sorted(actual, key=lambda r:r["threshold_mm"]),
            "input_audit": [r.asDict() for r in audit.collect()]}
        Path("reports/c_results/b_code_interoperability.json").write_text(json.dumps(output, indent=2)+"\n")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
