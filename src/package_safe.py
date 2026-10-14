"""Make a separately versioned v2 handoff; never overwrite the v1 release."""
import json
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
from src.task2.run import OUT, sha, save_json, fingerprints
from src.predict_safe import KNOTS, VERSION


def main():
    base = Path("output/c_handoff_v1.zip")
    expected = json.loads((OUT/"artifact_manifest.json").read_text())["sha256"]
    assert sha(base) == expected
    assert json.loads((OUT/"frozen_manifest.json").read_text())["hashes"] == fingerprints()
    assert json.loads((OUT/"safe_v2"/"acceptance.json").read_text())["passed"]
    sources = [Path(p) for p in ["src/predict_safe.py", "src/verify_safe.py", "src/package_safe.py", "tests/test_c_safe_inference.py"]]
    save_json(OUT/"safe_v2"/"interface_manifest.json", {"interface_version": VERSION,
        "base_v1_zip_sha256": expected, "v1_frozen_manifest_sha256": sha(OUT/"frozen_manifest.json"),
        "model_weights_changed": False, "v1_source_and_feature_hashes_unchanged": True,
        "fixed_knots_mm": KNOTS, "supported_threshold_mm": [0,10],
        "postprocessing": "equal-weight decreasing PAVA then linear interpolation in mm",
        "holdout_metrics": "not evaluated for v2; all existing test_*.json describe v1 only",
        "source_hashes": {str(p):sha(p) for p in sources}})
    overlays = {Path("README.md"), Path("requirements.txt")}
    for folder in ["src", "scripts", "tests", "configs", "docs", "reports", "data/samples"]:
        overlays.update(p for p in Path(folder).rglob("*") if p.is_file())
    excluded = OUT/"safe_v2"/"artifact_manifest.json"
    overlays = {p for p in overlays if p != excluded and p.suffix != ".pyc"
                and not any(x.startswith(".") or x == "__pycache__" for x in p.parts)}
    output = Path("output/c_handoff_v2.zip")
    with ZipFile(base) as source, ZipFile(output, "x", ZIP_DEFLATED, compresslevel=6) as target:
        for item in source.infolist():
            if Path(item.filename) not in overlays:
                target.writestr(item, source.read(item.filename))
        for path in sorted(overlays):
            target.write(path, str(path))
    with ZipFile(output) as result:
        assert result.testzip() is None
        count = len(result.namelist())
    checksum = sha(output)
    Path(str(output)+".sha256").write_text(f"{checksum}  {output.name}\n")
    save_json(excluded, {"path": str(output), "bytes": output.stat().st_size, "sha256": checksum,
        "members": count, "zip_crc_verified": True, "v1_zip_sha256": expected,
        "release_tag": "c-models-v2", "release_url": "https://github.com/ZYKuni/NUS_DSA5208_project2/releases/tag/c-models-v2",
        "download_url": "https://github.com/ZYKuni/NUS_DSA5208_project2/releases/download/c-models-v2/c_handoff_v2.zip",
        "note": "Separate post-hoc guarded interface; original v1 models/evidence retained. v1 test scores are not v2 scores."})


if __name__ == "__main__":
    main()
