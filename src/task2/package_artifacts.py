"""Package a locally verified C handoff; excludes raw data, virtualenv, and secrets."""
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
import json
from .run import OUT, MODELS, PREDS, FEATURES, fingerprints, sha, save_json


def main():
    assert (OUT/"test_complete.json").exists()
    assert json.loads((OUT/"reload_verification.json").read_text())["passed"]
    assert json.loads((OUT/"frozen_manifest.json").read_text())["hashes"] == fingerprints()
    roots = [Path(p) for p in ["src", "configs", "docs", "tests", "reports"]] + [MODELS, PREDS, FEATURES/"prepared"]
    paths = [Path(p) for p in ["README.md", "requirements.txt", "data/manifest.csv"]]
    for root in roots:
        paths.extend(p for p in root.rglob("*") if p.is_file())
    paths = sorted(set(p for p in paths if not any(part.startswith(".") or part == "__pycache__" for part in p.parts)
                       and p.suffix != ".pyc" and p != OUT/"artifact_manifest.json"))
    output = Path("output/c_handoff_v1.zip")
    output.parent.mkdir(exist_ok=True)
    with ZipFile(output, "x", compression=ZIP_DEFLATED, compresslevel=6) as archive:
        for path in paths:
            archive.write(path, str(path))
    with ZipFile(output) as archive:
        assert archive.testzip() is None
        members = archive.namelist()
    save_json(OUT/"artifact_manifest.json", {"path": str(output), "bytes": output.stat().st_size,
        "sha256": sha(output), "members": len(members), "zip_crc_verified": True,
        "includes": ["source/config/tests/docs/report aggregates/figures", "all candidate models and winner calibrators", "validation/calibration/test predictions", "sampled prepared feature table"],
        "excludes": ["raw and clean rainfall (reproduce with downloader/cleaner)", "full-population base labels", "virtualenv", "credentials", "group final PDF"],
        "note": "Local artifact; Git PR alone does not distribute model binaries. Upload this zip separately for team handoff/submission."})


if __name__ == "__main__":
    main()
