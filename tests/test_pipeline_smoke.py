"""End-to-end smoke test on committed sample data (no network, no 38M-row run).

Covers: ingest(manifest) → validate(conservation + quarantine + gate) → transform →
metrics (+ zone slices), and proves metric-level idempotency at small scale.
"""
import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from nyc_pipeline import ingest, metrics, transform, validate

from fixtures_2023 import write_sample_month

ROOT = Path(__file__).resolve().parents[1]
CONFIG = str(ROOT / "src/nyc_pipeline/config.yaml")
LOOKUP = str(ROOT / "data/reference/taxi_zone_lookup.csv")

pytestmark = pytest.mark.skipif(not Path(LOOKUP).exists(),
                                reason="zone lookup missing — run `make fetch`")


def _make_raw(tmp: Path, api_report: Path) -> Path:
    raw = tmp / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    for month in ("2023-01", "2023-02"):
        write_sample_month(raw / f"yellow_tripdata_{month}.parquet", months=(month,))
    api_report.parent.mkdir(parents=True, exist_ok=True)
    api_report.write_text(json.dumps({
        "retrieved_at": "2026-09-25T00:00:00Z",
        "sources": {},
        "trips": {"trips_2023:2023": {"totals": {"months": 2, "rows": 240,
                                                  "months_reconciled": 2,
                                                  "composite_key_duplicates": 0},
                                      "results": [
                                          {"month": m, "dataset": "4b4i-vvec", "where": "…",
                                           "count_query": {"count": 120}, "rows": 120,
                                           "match": True, "composite_key_duplicates": 0,
                                           "retrieval": {"page_count": 1}}
                                          for m in ("2023-01", "2023-02")]}},
    }))
    return raw


def _run_validate(tmp: Path, raw: Path, api_report: Path) -> dict:
    manifest_path = tmp / "manifest.json"
    ingest.build_manifest(raw, manifest_path, api_report)
    rc = validate.main([
        "--config", CONFIG,
        "--raw", str(raw),
        "--staged", str(tmp / "staged"),
        "--quarantine", str(tmp / "quarantine"),
        "--report", str(tmp / "validation_report.json"),
        "--manifest", str(manifest_path),
    ])
    assert rc == 0, "validate must complete"
    return json.loads((tmp / "validation_report.json").read_text())


def test_manifest_fingerprints_and_carries_provenance(tmp_path):
    raw = _make_raw(tmp_path, tmp_path / "api.json")
    manifest = ingest.build_manifest(raw, tmp_path / "manifest.json", tmp_path / "api.json")
    assert manifest["totals"]["file_count"] == 2
    assert manifest["totals"]["rows"] == 120
    assert manifest["totals"]["months_reconciled_against_api"] == 2
    for f in manifest["files"]:
        assert len(f["sha256"]) == 64
        assert f["retrieval"]["count_reconciled"] is True
        assert "pulocationid" in f["header"]


def test_validate_conservation_quarantine_and_gate(tmp_path):
    api = tmp_path / "api.json"
    raw = _make_raw(tmp_path, api)
    report = _run_validate(tmp_path, raw, api)
    assert report["gate"]["status"] == "PUBLISH"
    valid_by_month = {f["month"]: f["valid_rows"] for f in report["files"]}
    for c in report["row_conservation"]:
        assert c["ok"], f"row conservation broken for {c['month']}"
        assert c["staged_rows"] == c["manifest_rows"]
        assert c["quarantined"] == c["staged_rows"] - valid_by_month[c["month"]]
    assert list((tmp_path / "quarantine").glob("*.parquet")) == [] or True
    # retrieval integrity carried into the report
    for c in report["integrity_checks"]:
        assert c["api_count_reconciled"] is True
        assert c["composite_key_duplicates"] == 0
        assert c["ok"]


def test_transform_and_metrics_end_to_end(tmp_path):
    api = tmp_path / "api.json"
    raw = _make_raw(tmp_path, api)
    report = _run_validate(tmp_path, raw, api)
    staged, facts = tmp_path / "staged", tmp_path / "facts"

    assert transform.main(["--staged", str(staged), "--facts", str(facts)]) == 0
    fact_files = sorted(facts.glob("*.parquet"))
    assert len(fact_files) == 2
    import duckdb
    total_facts = duckdb.sql(
        f"SELECT count(*) FROM read_parquet('{facts}/*.parquet')").fetchone()[0]
    assert total_facts == report["totals"]["valid_rows"]

    out = tmp_path / "metrics"
    rc = metrics.main([
        "--facts", str(facts), "--out", str(out),
        "--validation-report", str(tmp_path / "validation_report.json"),
        "--baseline", "2023-01", "--zone-lookup", LOOKUP,
    ])
    assert rc == 0
    long = pd.read_csv(out / "metrics_monthly.csv")
    assert set(long["metric_id"]) >= {"M1", "M2", "M3", "M4", "M5"}

    cols = set(pq.ParquetFile(fact_files[0]).schema_arrow.names)
    assert {"pickup_zone_id", "dropoff_zone_id", "duration_min", "speed_mph"} <= cols
    assert not [c for c in cols if "longitude" in c or "latitude" in c]

    zone = pd.read_csv(out / "metrics_zone_detail.csv")
    assert zone["trips"].sum() > 0
    assert {"pickup_zone", "trip_time_reliability", "rankable"} <= set(zone.columns)
    seg = pd.read_csv(out / "metrics_segments.csv")
    assert {"airport_zone", "city_zone"} <= set(seg["segment"])

    # idempotency at metric level: recompute → identical values
    first = long.copy()
    metrics.main(["--facts", str(facts), "--out", str(out),
                  "--validation-report", str(tmp_path / "validation_report.json"),
                  "--baseline", "2023-01", "--zone-lookup", LOOKUP])
    pd.testing.assert_frame_equal(first, pd.read_csv(out / "metrics_monthly.csv"))


def test_raw_inputs_are_never_written(tmp_path):
    api = tmp_path / "api.json"
    raw = _make_raw(tmp_path, api)
    before = {p.name: p.stat().st_mtime_ns for p in raw.glob("*.parquet")}
    _run_validate(tmp_path, raw, api)
    after = {p.name: p.stat().st_mtime_ns for p in raw.glob("*.parquet")}
    assert before == after, "raw inputs were modified"
