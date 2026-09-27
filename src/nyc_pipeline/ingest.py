"""Stage 2 — INGEST: fingerprint the retrieved raw data, prove what we hold.

The raw inputs are the per-month parquet files written by `api_fetch` from the NYC Open Data
Socrata API. They are the *retrieved* bytes: every page response is preserved under
`data/api_raw/`, and each month was reconciled against the API's own `COUNT(*)` before the
parquet was written. This stage never modifies them — it only fingerprints and records
provenance so the rest of the pipeline (and any reviewer) can prove what was read.

Completeness proof chain: API COUNT(*) == rows fetched == parquet rows (api_fetch report)
→ manifest row_count per month → later reconciled against staged + quarantined rows.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

RAW_GLOB = "yellow_tripdata_*.parquet"
CHUNK = 8 * 1024 * 1024


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def fingerprint(path: Path) -> dict:
    """sha256 + size + row count + schema, read from parquet metadata (no data scan)."""
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(path)
    meta = pf.metadata
    return {
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
        "row_count": meta.num_rows,
        "row_groups": meta.num_row_groups,
        "header": list(pf.schema_arrow.names),
        "header_column_count": pf.schema_arrow.names.__len__(),
    }


def _api_provenance(report_path: Path) -> dict:
    """Per-month retrieval facts from the api_fetch report (count reconciliation, pages)."""
    if not report_path.exists():
        return {}
    report = json.loads(report_path.read_text())
    out: dict[str, dict] = {}
    for block in report.get("trips", {}).values():
        for res in block.get("results", []):
            month = res.get("month")
            if not month:
                continue
            out[month] = {
                "dataset": res.get("dataset"),
                "where": res.get("where"),
                "api_count": (res.get("count_query") or {}).get("count"),
                "rows_stored": res.get("rows"),
                "count_reconciled": res.get("match"),
                "pages": (res.get("retrieval") or {}).get("page_count"),
                "raw_pages_dir": (res.get("retrieval") or {}).get("raw_dir"),
                "composite_key_duplicates": res.get("composite_key_duplicates"),
            }
    return out


def build_manifest(raw_dir: Path, out_path: Path,
                   api_report: Path = Path("data/reference/api_retrieval_report.json")) -> dict:
    files = []
    provenance = _api_provenance(api_report)
    for p in sorted(raw_dir.glob(RAW_GLOB)):
        month = p.stem.replace("yellow_tripdata_", "")
        meta = fingerprint(p)
        meta.update({
            "name": p.name,
            "month": month,
            "raw_path": str(p),
            "source": "NYC Open Data (Socrata) — TLC trip records",
            "read_only_contract": "pipeline never writes to data/raw",
            "retrieval": provenance.get(month, {}),
        })
        files.append(meta)

    if not files:
        raise FileNotFoundError(f"no {RAW_GLOB} in {raw_dir} — run retrieval first")

    reconciled = [f for f in files if f["retrieval"].get("count_reconciled") is True]
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "retrieval_mode": "NYC Open Data Socrata API (paginated, per-month, count-reconciled)",
        "raw_dir": str(raw_dir),
        "files": files,
        "totals": {
            "file_count": len(files),
            "bytes": sum(f["bytes"] for f in files),
            "rows": sum(f["row_count"] for f in files),
            "months_reconciled_against_api": len(reconciled),
        },
        "assumptions": [
            "row_count comes from parquet metadata (row groups) — no data is modified.",
            "Per-month completeness is proven upstream in data/reference/api_retrieval_report.json "
            "(API COUNT(*) == rows fetched == parquet rows).",
        ],
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(manifest, indent=2))
    return manifest


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Fingerprint API-retrieved raw parquet and write manifest.json")
    ap.add_argument("--raw", default="data/raw")
    ap.add_argument("--manifest", default="data/manifest.json")
    ap.add_argument("--api-report", default="data/reference/api_retrieval_report.json")
    args = ap.parse_args(argv)

    manifest = build_manifest(Path(args.raw), Path(args.manifest), Path(args.api_report))
    t = manifest["totals"]
    print(f"mode: {manifest['retrieval_mode']}")
    for f in manifest["files"]:
        recon = f["retrieval"].get("count_reconciled")
        print(f"  {f['month']}: {f['row_count']:>10,} rows  {f['bytes']:>11,} B  "
              f"sha256={f['sha256'][:12]}…  api_reconciled={recon}")
    print(f"TOTAL: {t['rows']:,} rows across {t['file_count']} months "
          f"({t['months_reconciled_against_api']} reconciled against the API)")
    print(f"manifest → {args.manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
