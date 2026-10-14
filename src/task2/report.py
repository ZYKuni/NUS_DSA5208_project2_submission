"""Render bounded Spark aggregate outputs, never full observations, into C figures."""
import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from .run import OUT


def read(name):
    return json.loads((OUT/name).read_text())


def main():
    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False})
    figures = OUT/"figures"
    figures.mkdir(exist_ok=True)
    selected = read("frozen_manifest.json")["selection"]
    names = ["M0", *[r["name"] for r in selected["family_best"].values()], "serving"]
    results = {name: read(f"test_{name}.json") for name in names}
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), constrained_layout=True)
    for name, result in results.items():
        rows = result["raw" if name == "M0" else "calibrated"]["metrics"]
        for ax, key, title in zip(axes, ["brier", "pr_auc"], ["Brier score (lower is better)", "PR-AUC (higher is better)"]):
            ax.plot([str(r["threshold_mm"]) for r in rows], [r[key] for r in rows], marker="o", label=name)
            ax.set(xlabel="Rainfall threshold (mm; categorical spacing)", ylabel=title, title=title)
            ax.grid(alpha=.2)
    axes[0].legend(fontsize=8)
    fig.suptitle("2024 holdout • fixed 0.5% origin sample • final calibrated outputs")
    fig.savefig(figures/"test_by_threshold.png", dpi=180)
    plt.close(fig)

    fig, axes = plt.subplots(1, 3, figsize=(12, 3.8), constrained_layout=True)
    primary = results["serving"]
    for ax, q in zip(axes, [0., 1., 5.]):
        for source, symbol in [("raw", "o"), ("calibrated", "s")]:
            rows = sorted([r for r in primary[source]["reliability"] if r["threshold_mm"] == q], key=lambda r:r["bin"])
            ax.plot([r["mean_probability"] for r in rows], [r["event_rate"] for r in rows], marker=symbol, label=source)
        ax.plot([0, 1], [0, 1], "--", color="gray", linewidth=1)
        ax.set(xlim=(0, 1), ylim=(0, 1), xlabel="Mean predicted probability", ylabel="Observed frequency", title=f"q = {q:g} mm")
        ax.grid(alpha=.2)
    axes[0].legend()
    fig.suptitle("Serving rule reliability • 2024 holdout • 10 fixed-width bins; sparse bins are uncertain")
    fig.savefig(figures/"serving_reliability.png", dpi=180)
    plt.close(fig)

    rows = primary["calibrated"]["by_group"]["month"]
    fig, ax = plt.subplots(figsize=(8, 3.8), constrained_layout=True)
    for q in [0., 1., 5.]:
        part = sorted([r for r in rows if r["threshold_mm"] == q], key=lambda r:r["month"])
        ax.plot([r["month"] for r in part], [r["brier"] for r in part], marker="o", label=f"q={q:g} mm")
    ax.set(xlabel="Month in 2024", ylabel="Brier score", title="Serving rule: monthly performance (weather mix also changes)", xticks=range(1,13))
    ax.grid(alpha=.2)
    ax.legend()
    fig.savefig(figures/"serving_monthly.png", dpi=180)
    plt.close(fig)
    # Small Markdown tables are directly embeddable in the group report.
    lines = ["# C result tables (generated)", "", "## Validation selection", "",
             "| Model | Six-threshold macro Brier | Fit + evaluation seconds |", "|---|---:|---:|"]
    for row in read("experiments.json")["runs"]:
        lines.append(f"| {row['name']} | {row['macro_brier']:.6f} | {row['seconds']:.1f} |")
    lines += ["", "## 2024 serving rule (calibrated)", "", "| q (mm) | Origins | Events | Event rate | Brier | PR-AUC | ROC-AUC | Log loss |", "|---:|---:|---:|---:|---:|---:|---:|---:|"]
    fmt = lambda v: "undefined" if v is None else f"{v:.6f}"
    for row in primary["calibrated"]["metrics"]:
        lines.append(f"| {row['threshold_mm']:g} | {row['n']} | {int(row['positive_count'])} | {fmt(row['positive_rate'])} | {fmt(row['brier'])} | {fmt(row['pr_auc'])} | {fmt(row['roc_auc'])} | {fmt(row['log_loss'])} |")
    lines += ["", "## Test mean Brier over the six trained thresholds", "", "| Model | Raw | Calibrated |", "|---|---:|---:|"]
    qs = read("frozen_manifest.json")["config"]["training_thresholds"]
    for name, result in results.items():
        means = {source: sum(r['brier'] for r in metrics['metrics'] if r['threshold_mm'] in qs)/len(qs) for source,metrics in result.items()}
        lines.append(f"| {name} | {fmt(means['raw'])} | {fmt(means.get('calibrated'))} |")
    lines += ["", "Selection was completed on validation, not this test table. Calibration-fit scores are in-sample and are excluded here.", ""]
    (OUT/"result_tables.md").write_text("\n".join(lines))


if __name__ == "__main__":
    main()
