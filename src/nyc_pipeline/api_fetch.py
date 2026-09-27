"""Stage 1 — RETRIEVAL: NYC Open Data (Socrata) API + TLC HTTP file.

Rules this stage lives by:
  - a 200 on one page proves nothing; completeness = COUNT(*) == rows saved, per month
  - every raw API page is preserved byte-for-byte (gzip is storage, not transformation)
  - months are partitioned by pickup timestamp and paged with an explicit $order
  - 429/5xx/timeouts are retried with backoff; a failed count is recorded UNKNOWN, not 0
  - resumable: a month whose parquet exists and reconciled is skipped on re-runs
  - conversion to parquet only re-types the page payloads; values are never edited

Source: NYC Open Data dataset 4b4i-vvec "2023 Yellow Taxi Trip Data" (TLC), plus the
TLC zone-lookup CSV and an API cross-check of the zone reference.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests

SOCRATA_BASE = "https://data.cityofnewyork.us/resource"
ZONE_CSV_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

DATASETS = {
    "trips_2023": "4b4i-vvec",   # 2023 Yellow Taxi Trip Data (native zone ids, no lat/lon)
    "taxi_zones": "8meu-9t5y",    # zone reference (names/boroughs cross-check)
}

PAGE_SIZE = 50_000               # Socrata SODA max per request
PICKUP = "tpep_pickup_datetime"

# canonical parquet types: everything is read as VARCHAR then cast, so the schema does not
# depend on what the CSV sniffer happens to infer per page
CASTS = {
    PICKUP: "TIMESTAMP",
    "tpep_dropoff_datetime": "TIMESTAMP",
    "trip_distance": "DOUBLE",
    "fare_amount": "DOUBLE",
    "tip_amount": "DOUBLE",
    "tolls_amount": "DOUBLE",
    "extra": "DOUBLE",
    "mta_tax": "DOUBLE",
    "imp_surcharge": "DOUBLE",
    "improvement_surcharge": "DOUBLE",
    "airport_fee": "DOUBLE",
    "congestion_surcharge": "DOUBLE",
    "total_amount": "DOUBLE",
    "passenger_count": "BIGINT",
    "ratecodeid": "BIGINT",
    "payment_type": "BIGINT",
    "vendorid": "BIGINT",
    "store_and_fwd_flag": "VARCHAR",
    "pulocationid": "BIGINT",
    "dolocationid": "BIGINT",
}
# columns used for the paging-uniqueness diagnostic (no trip id exists in TLC data)
COMPOSITE_KEY = [PICKUP, "tpep_dropoff_datetime", "pulocationid", "dolocationid",
                 "trip_distance", "fare_amount", "total_amount"]
ORDER_BY = ",".join(COMPOSITE_KEY)

RETRYABLE = {429, 500, 502, 503, 504}


def _get(url: str, params: dict | None = None, *, retries: int = 5, timeout: int = 120) -> requests.Response:
    delay = 1.0
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code in RETRYABLE:
                last_exc = RuntimeError(f"HTTP {resp.status_code} from {url}")
            else:
                resp.raise_for_status()
                return resp
        except (requests.Timeout, requests.ConnectionError, RuntimeError) as e:
            last_exc = e
        if attempt < retries:
            time.sleep(delay)
            delay = min(delay * 2, 30)
    raise RuntimeError(f"GET {url} failed after {retries} attempts: {last_exc}")


def socrata_count(dataset: str, where: str | None = None, timeout: int = 420) -> dict:
    """COUNT(*) via SoQL. On timeout/failure returns count=None + status (UNKNOWN, not 0)."""
    params = {"$select": "count(*)"}
    if where:
        params["$where"] = where
    started = time.time()
    try:
        resp = _get(f"{SOCRATA_BASE}/{dataset}.json", params, retries=2, timeout=timeout)
        return {"count": int(resp.json()[0]["count"]), "status": "OK",
                "seconds": round(time.time() - started, 1)}
    except Exception as e:  # noqa: BLE001 - recorded, not swallowed
        return {"count": None, "status": f"UNKNOWN ({type(e).__name__}: {e})",
                "seconds": round(time.time() - started, 1)}


def month_window(year: int, month: int) -> dict:
    start = f"{year:04d}-{month:02d}-01T00:00:00"
    ny, nm = (year + 1, 1) if month == 12 else (year, month + 1)
    end = f"{ny:04d}-{nm:02d}-01T00:00:00"
    return {"month": f"{year:04d}-{month:02d}", "start": start, "end": end,
            "where": f"{PICKUP} >= '{start}' AND {PICKUP} < '{end}'"}


def fetch_all_pages(dataset: str, where: str | None, order: str | None,
                    page_size: int, out_dir: Path, *, select: str | None = None,
                    max_rows: int | None = None) -> dict:
    """Paginate $limit/$offset as JSON, saving every raw page verbatim (reference data)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    offset, page, rows = 0, 0, 0
    pages = []
    while True:
        params: dict = {"$limit": page_size, "$offset": offset}
        if where:
            params["$where"] = where
        if order:
            params["$order"] = order
        if select:
            params["$select"] = select
        resp = _get(f"{SOCRATA_BASE}/{dataset}.json", params)
        payload = resp.content                      # raw bytes, preserved as received
        page += 1
        page_path = out_dir / f"page_{page:04d}.json"
        page_path.write_bytes(payload)
        n = len(resp.json())
        pages.append({"page": page, "rows": n, "file": page_path.name,
                      "bytes": len(payload), "http": resp.status_code})
        rows += n
        if n < page_size or (max_rows is not None and rows >= max_rows):
            break
        offset += page_size
    return {"dataset": dataset, "pages": pages, "page_count": page, "rows_fetched": rows,
            "raw_dir": str(out_dir)}


def fetch_month_pages(dataset: str, window: dict, raw_dir: Path, page_size: int,
                      *, page_workers: int = 3, max_rows: int | None = None) -> dict:
    """Fetch one month as gzipped CSV pages (raw response bytes, gzip-compressed for disk).

    Resumable: existing pages are counted and paging continues at the next offset.
    Each page is written atomically (.tmp then rename) so an interrupted run cannot leave a
    half-written page that a later run would trust. Pages are independent (offset paging), so
    they are fetched in waves of ``page_workers``; the wave stops at the first short page,
    which is how the end of the month is detected.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    existing = sorted(raw_dir.glob("page_*.csv.gz"))
    offset, rows, page = len(existing) * page_size, 0, len(existing) + 1
    for f in existing:                                   # resume: trust pages already on disk
        with gzip.open(f, "rt") as fh:
            rows += max(sum(1 for _ in fh) - 1, 0)
    started = time.time()
    url = f"{SOCRATA_BASE}/{dataset}.csv"

    def get(off: int) -> tuple[int, bytes, int]:
        resp = _get(url, {"$limit": page_size, "$offset": off, "$where": window["where"],
                          "$order": ORDER_BY}, timeout=300)
        body = resp.content
        n = max(body.decode("utf-8", errors="replace").count("\n") - 1, 0)
        return off, body, n

    while True:
        offsets = [offset + i * page_size for i in range(page_workers)]
        with ThreadPoolExecutor(max_workers=page_workers) as pool:
            fetched = list(pool.map(get, offsets))
        wrote, reached_end = 0, False
        for off, body, n in fetched:                     # keep order; stop at first short page
            if off != offset + wrote * page_size:
                break
            path = raw_dir / f"page_{page + wrote:04d}.csv.gz"
            tmp = path.with_suffix(".gz.tmp")
            with gzip.open(tmp, "wb") as gz:
                gz.write(body)
            tmp.rename(path)
            rows += n
            wrote += 1
            if n < page_size:
                reached_end = True
                break
        offset += wrote * page_size
        page += wrote
        if reached_end or wrote == 0 or (max_rows is not None and rows >= max_rows):
            break

    return {"raw_dir": str(raw_dir), "page_count": len(list(raw_dir.glob("page_*.csv.gz"))),
            "rows_fetched": rows, "page_size": page_size, "page_workers": page_workers,
            "order_by": ORDER_BY, "seconds": round(time.time() - started, 1)}


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def convert_month(window: dict, raw_dir: Path, out_parquet: Path) -> dict:
    """Re-type the raw pages into one parquet per month. Values are cast, never edited."""
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    files = sorted(raw_dir.glob("page_*.csv.gz"))
    if not files:
        raise FileNotFoundError(f"no pages in {raw_dir}")
    header = next(csv.reader([gzip.open(files[0], "rt").readline()]))
    columns_map = ", ".join(f"'{c}': 'VARCHAR'" for c in header)
    select = ", ".join(
        f"try_cast({q} AS {CASTS[c]}) AS {q}" if c in CASTS else q
        for c in header for q in [f'"{c}"'])
    # order only by columns this source actually has (keeps paging deterministic)
    order_cols = [c for c in ORDER_BY.split(",") if c in header]
    order_by = ", ".join(order_cols) if order_cols else header[0]
    glob = str(raw_dir / "page_*.csv.gz")
    con = duckdb.connect()
    con.execute("SET threads TO 4")
    tmp = out_parquet.with_suffix(".parquet.tmp")
    con.execute(f"""
        COPY (SELECT {select}
              FROM read_csv('{glob}', header=true, columns={{{columns_map}}},
                            union_by_name=true)
              ORDER BY {order_by})
        TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    con.close()
    tmp.replace(out_parquet)
    rows, dupes = _parquet_audit(out_parquet)
    return {"parquet": str(out_parquet), "rows": rows, "bytes": out_parquet.stat().st_size,
            "sha256": _sha256(out_parquet), "columns": header,
            "composite_key_duplicates": dupes}


def _parquet_audit(path: Path) -> tuple[int, int]:
    """Row count + duplicate count on the composite key (diagnostic for offset paging:
    unstable $order would show up as duplicated composite keys). Uses only the key columns
    this file actually has, so it also works on partial test fixtures."""
    import pyarrow.parquet as pq
    have = set(pq.ParquetFile(path).schema_arrow.names)
    key_cols = [c for c in COMPOSITE_KEY if c in have]
    con = duckdb.connect()
    if not key_cols:
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]
        con.close()
        return int(rows), 0
    key = ", ".join(f'"{c}"' for c in key_cols)
    row = con.execute(f"""
        SELECT count(*),
               count(*) - count(DISTINCT ({key}))
        FROM read_parquet('{path}')
    """).fetchone()
    con.close()
    return int(row[0]), int(row[1])


def fetch_month(dataset: str, year: int, month: int, raw_root: Path, parquet_dir: Path,
                page_size: int = PAGE_SIZE, *, page_workers: int = 3, force: bool = False,
                max_rows: int | None = None) -> dict:
    """Full per-month retrieval: count → pages → parquet → reconciliation."""
    window = month_window(year, month)
    label = window["month"]
    raw_dir = raw_root / dataset / str(year) / label
    out_parquet = parquet_dir / f"yellow_tripdata_{label}.parquet"
    started = time.time()

    if out_parquet.exists() and not force and not max_rows:
        # resume path: skip the download but still prove the stored month against the API
        expected = socrata_count(dataset, window["where"])
        rows, dupes = _parquet_audit(out_parquet)
        match = expected["count"] is not None and expected["count"] == rows
        return {"month": label, "status": "SKIPPED (parquet present)", "dataset": dataset,
                "where": window["where"], "count_query": expected, "rows": rows,
                "composite_key_duplicates": dupes, "match": match,
                "verdict": ("COMPLETE: COUNT(*) == parquet rows" if match else
                            f"INCOMPLETE/UNKNOWN (count={expected['count']}, rows={rows})"),
                "parquet": str(out_parquet), "sha256": _sha256(out_parquet),
                "bytes": out_parquet.stat().st_size, "seconds": 0.0}

    expected = socrata_count(dataset, window["where"])
    pages = fetch_month_pages(dataset, window, raw_dir, page_size,
                              page_workers=page_workers, max_rows=max_rows)
    conv = convert_month(window, raw_dir, out_parquet)
    match = expected["count"] is not None and expected["count"] == conv["rows"]
    if max_rows:                      # test/smoke path: reconciliation not expected
        match = None
    return {
        "month": label, "status": "OK", "dataset": dataset, "where": window["where"],
        "count_query": expected, "retrieval": pages, "parquet": conv["parquet"],
        "rows": conv["rows"], "bytes": conv["bytes"], "sha256": conv["sha256"],
        "columns": conv["columns"], "composite_key_duplicates": conv["composite_key_duplicates"],
        "match": match,
        "verdict": ("COMPLETE: COUNT(*) == parquet rows" if match else
                    (f"INCOMPLETE/UNKNOWN (count={expected['count']}, rows={conv['rows']})"
                     if match is not True else "SKIPPED reconciliation (max_rows)")),
        "seconds": round(time.time() - started, 1),
    }


def run_year_retrieval(dataset: str, year: int, months: list[int], raw_root: Path,
                       parquet_dir: Path, workers: int = 4, page_size: int = PAGE_SIZE,
                       page_workers: int = 3, force: bool = False,
                       max_rows: int | None = None) -> dict:
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(fetch_month, dataset, year, m, raw_root, parquet_dir,
                               page_size, page_workers=page_workers, force=force,
                               max_rows=max_rows) for m in months]
        for fut in futures:
            res = fut.result()
            results.append(res)
            print(f"[api] {res['month']}: {res.get('rows', 0):>10,} rows  "
                  f"{(res.get('verdict') or res['status'])}  ({res['seconds']}s)", flush=True)
    results.sort(key=lambda r: r["month"])
    reconciled = [r for r in results if r.get("match") is True]
    return {
        "dataset": dataset, "year": year, "months": months, "results": results,
        "totals": {
            "months": len(results),
            "rows": sum(r.get("rows", 0) for r in results),
            "months_reconciled": len(reconciled),
            "composite_key_duplicates": sum(r.get("composite_key_duplicates", 0) for r in results),
        },
        "verdict": ("ALL MONTHS RECONCILED: COUNT(*) == parquet rows"
                    if len(reconciled) == len(results) and results
                    else f"{len(reconciled)}/{len(results)} months reconciled"),
    }


def run_reference_retrieval(out_dir: Path) -> dict:
    """Zone reference: TLC CSV download + API cross-check (names/boroughs only, no polygons)."""
    results = {"retrieved_at": datetime.now(timezone.utc).isoformat(), "sources": {}}
    results["sources"]["zone_lookup_csv"] = fetch_http_file(
        ZONE_CSV_URL, out_dir / "taxi_zone_lookup.csv")
    results["sources"]["zone_lookup_api"] = fetch_all_pages(
        DATASETS["taxi_zones"], where=None, order="locationid", page_size=1000,
        out_dir=out_dir / "socrata_taxi_zones",
        select="locationid,zone,borough")
    results["sources"]["zone_count_check"] = socrata_count(DATASETS["taxi_zones"], timeout=120)
    return results


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Retrieve NYC TLC trip records from the NYC Open Data Socrata API")
    ap.add_argument("--dataset", default="trips_2023", choices=sorted(DATASETS))
    ap.add_argument("--year", type=int, default=2023)
    ap.add_argument("--months", nargs="*", type=int, help="subset (default: all 12)")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--page-size", type=int, default=PAGE_SIZE)
    ap.add_argument("--page-workers", type=int, default=3,
                    help="concurrent page requests inside one month")
    ap.add_argument("--out", default="data/reference")
    ap.add_argument("--raw", default="data/raw")
    ap.add_argument("--raw-pages", default="data/api_raw")
    ap.add_argument("--max-rows", type=int, help="stop early (smoke tests)")
    ap.add_argument("--force", action="store_true", help="re-fetch months that already exist")
    ap.add_argument("--skip-reference", action="store_true",
                    help="skip the zone-reference retrieval (already committed)")
    args = ap.parse_args(argv)

    out_dir = Path(args.out)
    report_path = out_dir / "api_retrieval_report.json"
    report = json.loads(report_path.read_text()) if report_path.exists() else {
        "retrieved_at": datetime.now(timezone.utc).isoformat(), "sources": {}}

    if not args.skip_reference:
        report["sources"] = run_reference_retrieval(out_dir)["sources"]

    months = args.months or list(range(1, 13))
    dataset_id = DATASETS[args.dataset]
    trips = run_year_retrieval(dataset_id, args.year, months, Path(args.raw_pages),
                               Path(args.raw), workers=args.workers,
                               page_size=args.page_size, page_workers=args.page_workers,
                               force=args.force, max_rows=args.max_rows)
    if args.max_rows:
        # smoke path: one month only, no reconciliation claim
        report["smoke_test"] = trips
    else:
        report["trips"] = {**report.get("trips", {}), f"{args.dataset}:{args.year}": trips}

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2))

    t = trips["totals"]
    print(f"months {t['months']} | rows {t['rows']:,} | reconciled {t['months_reconciled']}"
          f" | composite-key dupes {t['composite_key_duplicates']:,}")
    print(f"verdict: {trips['verdict']}")
    print(f"report → {report_path}")
    return 0 if trips["totals"]["months_reconciled"] == len(trips["results"]) else 2


if __name__ == "__main__":
    raise SystemExit(main())
