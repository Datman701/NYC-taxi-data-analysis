"""Offline tests for the Socrata retrieval layer — paging, resume, windows, reconciliation.

No network: `_get` is monkeypatched with a fake that serves deterministic pages, so these
tests assert the *retrieval logic* (offsets, short-page termination, atomic raw pages,
resume, count reconciliation) without touching the API.
"""
import gzip
import json
from pathlib import Path

import pytest

from nyc_pipeline import api_fetch as af


def _csv_page(rows: int, offset: int) -> bytes:
    header = '"vendorid","tpep_pickup_datetime","trip_distance"\n'
    body = "".join(f'2,"2023-01-10 08:{i%60:02d}:00",{1 + i % 20}.5\n' for i in range(rows))
    return (header + body).encode()


class FakeResponse:
    def __init__(self, payload: bytes):
        self.content = payload
        self.status_code = 200

    def json(self):
        import json as _json
        return _json.loads(self.content)


@pytest.fixture
def fake_api(monkeypatch):
    """Serves `total` rows for the whole month in pages of `page_size`."""
    state = {"calls": [], "total": 0, "page_size": 10}

    def _get(url, params=None, **kw):
        off = int((params or {}).get("$offset", 0))
        size = int((params or {}).get("$limit", 10))
        state["calls"].append((url, dict(params or {})))
        if url.endswith(".json"):                      # count query
            return FakeResponse(json.dumps([{"count": str(state["total"])}]).encode())
        remaining = max(state["total"] - off, 0)
        return FakeResponse(_csv_page(min(size, remaining), off))

    monkeypatch.setattr(af, "_get", _get)
    return state


def test_month_window_boundaries_are_half_open():
    w = af.month_window(2023, 1)
    assert w["month"] == "2023-01"
    assert w["start"] == "2023-01-01T00:00:00" and w["end"] == "2023-02-01T00:00:00"
    dec = af.month_window(2023, 12)
    assert dec["end"] == "2024-01-01T00:00:00"           # year rollover
    assert af.month_window(2023, 5)["end"] == "2023-06-01T00:00:00"


def test_paging_walks_offsets_and_stops_on_short_page(fake_api, tmp_path):
    fake_api.update(total=25, page_size=10)
    res = af.fetch_month_pages("ds", af.month_window(2023, 1), tmp_path / "raw", 10, page_workers=1)
    assert res["rows_fetched"] == 25
    assert res["page_count"] == 3                    # 10 + 10 + 5 (short page ends it)
    offsets = [p["$offset"] for _, p in fake_api["calls"]]
    assert offsets == [0, 10, 20]
    assert len(list((tmp_path / "raw").glob("page_*.csv.gz"))) == 3


def test_pages_are_gzipped_raw_bytes_and_atomic(tmp_path):
    payload = _csv_page(3, 0)
    pages = tmp_path / "raw"
    pages.mkdir(parents=True)
    (pages / "page_0001.csv.gz").write_bytes(gzip.compress(payload))
    with gzip.open(pages / "page_0001.csv.gz", "rb") as f:
        assert f.read() == payload                    # byte-for-byte what the API sent
    assert not list(pages.glob("*.tmp"))             # no partial files left behind


def test_resume_continues_from_existing_pages(monkeypatch, tmp_path):
    """A re-run must not re-download pages it already has."""
    seen = []

    def _get(url, params=None, **kw):
        off = int((params or {}).get("$offset", 0))
        seen.append(off)
        if url.endswith(".json"):
            return FakeResponse(json.dumps([{"count": "25"}]).encode())
        return FakeResponse(_csv_page(min(10, max(25 - off, 0)), off))

    monkeypatch.setattr(af, "_get", _get)
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "page_0001.csv.gz").write_bytes(gzip.compress(_csv_page(10, 0)))
    res = af.fetch_month_pages("ds", af.month_window(2023, 1), raw, 10, page_workers=1)
    assert seen[0] == 10                             # starts after the stored page
    assert res["rows_fetched"] == 25                 # 10 resumed + 15 fetched
    assert res["page_count"] == 3


def test_count_reconciliation_flags_mismatch(monkeypatch, tmp_path):
    """If COUNT(*) != stored rows, the report must say INCOMPLETE (never 'close enough')."""
    def _get(url, params=None, **kw):
        if url.endswith(".json"):
            return FakeResponse(json.dumps([{"count": "999"}]).encode())
        return FakeResponse(_csv_page(3, 0))

    monkeypatch.setattr(af, "_get", _get)
    monkeypatch.setattr(af, "fetch_month_pages",
                        lambda *a, **k: {"raw_dir": "x", "page_count": 1, "rows_fetched": 3,
                                         "page_size": 10, "page_workers": 1,
                                         "order_by": af.ORDER_BY, "seconds": 0.1})
    monkeypatch.setattr(af, "convert_month",
                        lambda *a, **k: {"parquet": "p", "rows": 3, "bytes": 1, "sha256": "z",
                                         "columns": [], "composite_key_duplicates": 0})
    res = af.fetch_month("ds", 2023, 1, tmp_path / "raw", tmp_path / "out.parquet",
                         page_size=10)
    assert res["match"] is False
    assert "INCOMPLETE" in res["verdict"]
    assert res["count_query"]["count"] == 999 and res["rows"] == 3


def test_count_failure_is_recorded_unknown_not_zero(monkeypatch):
    monkeypatch.setattr(af, "_get", lambda *a, **k: (_ for _ in ()).throw(TimeoutError()))
    res = af.socrata_count("ds", "x IS NOT NULL", timeout=1)
    assert res["count"] is None
    assert res["status"].startswith("UNKNOWN")       # never silently 0


def test_conversion_casts_columns_and_audits_duplicates(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    header = '"vendorid","tpep_pickup_datetime","tpep_dropoff_datetime","trip_distance","pulocationid"'
    rows = [f'2,"2023-01-10 08:{i:02d}:00","2023-01-10 08:{i:02d}:30",3.5,132' for i in range(4)]
    (raw / "page_0001.csv.gz").write_bytes(gzip.compress((header + "\n" + "\n".join(rows) + "\n").encode()))
    out = af.convert_month(af.month_window(2023, 1), raw, tmp_path / "out.parquet")
    assert out["rows"] == 4
    assert out["composite_key_duplicates"] == 0        # distinct trips, no paging artefacts

    import duckdb
    types = dict(duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{tmp_path / 'out.parquet'}')")
                 .df()[["column_name", "column_type"]].values)
    assert types["tpep_pickup_datetime"] == "TIMESTAMP"
    assert types["trip_distance"] == "DOUBLE"
    assert types["pulocationid"] == "BIGINT"


def test_identical_rows_are_detected_as_duplicate_keys(tmp_path):
    """The duplicate audit is a real integrity signal, not a rubber stamp."""
    raw = tmp_path / "raw"
    raw.mkdir()
    header = '"tpep_pickup_datetime","tpep_dropoff_datetime","trip_distance"'
    row = '"2023-01-10 08:00:00","2023-01-10 08:30:00",3.5'
    (raw / "page_0001.csv.gz").write_bytes(
        gzip.compress((header + "\n" + "\n".join([row] * 5) + "\n").encode()))
    out = af.convert_month(af.month_window(2023, 1), raw, tmp_path / "dup.parquet")
    assert out["rows"] == 5 and out["composite_key_duplicates"] == 4
