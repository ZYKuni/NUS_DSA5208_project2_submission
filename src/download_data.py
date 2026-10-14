"""P1-01: download the 2017–2024 annual CSVs and record a manifest.

Usage:
    python src/download_data.py --config configs/project.yaml [--years 2017 2024] [--force]

For every year this writes data/raw/rainfall_<year>.csv (never modified
afterwards) and one row in data/manifest.csv with source URL, dataset id,
download time, bytes, SHA-256, header and line-based row count. Exact
parsed row counts come later from `data_quality.py --stage raw`.
"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import os
import sys
import time
from pathlib import Path

import requests

from common import base_arg_parser, load_config, repo_path, setup_logging

CHUNK = 8 * 1024 * 1024
MANIFEST_FIELDS = [
    "year", "dataset_id", "dataset_name", "source_page", "metadata_last_updated",
    "download_started_utc", "download_finished_utc", "file_name", "bytes",
    "sha256", "header", "header_matches_expected", "line_based_rows",
]


def _session(cfg: dict) -> requests.Session:
    s = requests.Session()
    key = os.environ.get(cfg["source"].get("api_key_env", ""), "")
    if key:
        s.headers["x-api-key"] = key
    s.headers["User-Agent"] = "nus-dsa5208-project2/1.0"
    return s


def _get_json(s: requests.Session, url: str, log, tries: int = 8) -> dict:
    delay = 5.0
    for attempt in range(1, tries + 1):
        r = s.get(url, timeout=60)
        if r.status_code == 429 or r.status_code >= 500:
            log.warning("HTTP %s for %s (attempt %d); retrying in %.0fs",
                        r.status_code, url, attempt, delay)
            time.sleep(delay)
            delay = min(delay * 2, 120)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"giving up on {url}")


def resolve_download_url(s, cfg, dataset_id, log) -> str:
    src = cfg["source"]
    _get_json(s, src["initiate_url"].format(dataset_id=dataset_id), log)
    for _ in range(60):
        body = _get_json(s, src["poll_url"].format(dataset_id=dataset_id), log)
        data = body.get("data") or {}
        if data.get("url"):
            return data["url"]
        log.info("poll %s: %s", dataset_id, data.get("status") or body)
        time.sleep(5)
    raise RuntimeError(f"no download URL for {dataset_id}")


def fetch_metadata(s, cfg, dataset_id, log) -> dict:
    body = _get_json(s, cfg["source"]["metadata_url"].format(dataset_id=dataset_id), log)
    return body.get("data") or body


def stream_to_file(s, url: str, dest: Path, log) -> None:
    """Stream to <dest>.part, resuming with HTTP Range where possible."""
    part = dest.with_suffix(dest.suffix + ".part")
    pos = part.stat().st_size if part.exists() else 0
    headers = {"Range": f"bytes={pos}-"} if pos else {}
    with s.get(url, stream=True, timeout=120, headers=headers) as r:
        if pos and r.status_code != 206:
            log.info("server ignored Range; restarting %s", dest.name)
            pos = 0
        r.raise_for_status()
        total = r.headers.get("Content-Length")
        total = int(total) + pos if total else None
        mode = "ab" if pos else "wb"
        done, last = pos, time.time()
        with open(part, mode) as fh:
            for chunk in r.iter_content(CHUNK):
                fh.write(chunk)
                done += len(chunk)
                if time.time() - last > 15:
                    pct = f"{100 * done / total:.1f}%" if total else "?"
                    log.info("%s: %.0f MB (%s)", dest.name, done / 1e6, pct)
                    last = time.time()
    if total is not None and part.stat().st_size != total:
        raise RuntimeError(f"{dest.name}: got {part.stat().st_size} bytes, expected {total}")
    part.rename(dest)


def file_facts(path: Path) -> tuple[str, int, str]:
    """SHA-256, newline count, and header line in a single pass."""
    h = hashlib.sha256()
    lines = 0
    header = None
    with open(path, "rb") as fh:
        while chunk := fh.read(CHUNK):
            if header is None:
                header = chunk.split(b"\n", 1)[0].decode("utf-8-sig").strip()
            h.update(chunk)
            lines += chunk.count(b"\n")
    return h.hexdigest(), lines, header or ""


def read_manifest(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    with open(path, newline="", encoding="utf-8") as fh:
        return {row["year"]: row for row in csv.DictReader(fh)}


def write_manifest(path: Path, rows: dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=MANIFEST_FIELDS)
        w.writeheader()
        for year in sorted(rows):
            w.writerow({k: rows[year].get(k, "") for k in MANIFEST_FIELDS})


def main() -> int:
    parser = base_arg_parser(__doc__)
    parser.add_argument("--years", nargs="*", type=int)
    parser.add_argument("--force", action="store_true", help="re-download existing files")
    args = parser.parse_args()
    cfg = load_config(args.config)
    log = setup_logging(cfg, "download_data")

    raw_dir = repo_path(cfg, "raw_dir")
    raw_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = repo_path(cfg, "manifest")
    manifest = read_manifest(manifest_path)
    datasets = {int(k): v for k, v in cfg["source"]["datasets"].items()}
    years = args.years or sorted(datasets)
    expected = ",".join(cfg["source"]["expected_columns"])
    s = _session(cfg)

    for year in years:
        dataset_id = datasets[year]
        dest = raw_dir / f"rainfall_{year}.csv"
        meta = fetch_metadata(s, cfg, dataset_id, log)
        name = meta.get("name", "")
        if f"({year})" not in name:
            raise RuntimeError(f"dataset {dataset_id} is named {name!r}, expected year {year}")

        started = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        if dest.exists() and not args.force:
            log.info("%s exists; recomputing facts only (use --force to re-download)", dest.name)
            prev = manifest.get(str(year), {})
            started = prev.get("download_started_utc", "")
            finished = prev.get("download_finished_utc", "")
        else:
            url = resolve_download_url(s, cfg, dataset_id, log)
            log.info("downloading %s (%s)", dest.name, dataset_id)
            stream_to_file(s, url, dest, log)
            finished = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

        sha, lines, header = file_facts(dest)
        manifest[str(year)] = {
            "year": year,
            "dataset_id": dataset_id,
            "dataset_name": name,
            "source_page": f"https://data.gov.sg/datasets/{dataset_id}/view",
            "metadata_last_updated": meta.get("lastUpdatedAt", ""),
            "download_started_utc": started,
            "download_finished_utc": finished,
            "file_name": dest.name,
            "bytes": dest.stat().st_size,
            "sha256": sha,
            "header": header,
            "header_matches_expected": header.replace('"', "") == expected,
            "line_based_rows": lines - 1,
        }
        write_manifest(manifest_path, manifest)
        log.info("%s: %d bytes, %d data lines, sha256 %s", dest.name,
                 dest.stat().st_size, lines - 1, sha[:12])
        if header.replace('"', "") != expected:
            log.error("%s header differs from expected columns: %s", dest.name, header)
    return 0


if __name__ == "__main__":
    sys.exit(main())
