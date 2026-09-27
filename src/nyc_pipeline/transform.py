"""Stage 4 — TRANSFORM: staged (validated) rows → trip-level fact table.

One fact row = one valid trip (grain A1). Derived fields are computed here, never
upstream in analysis, so every notebook and metric sees the same definitions.

The 2023 source carries TLC's own zone ids, so `pickup_zone_id` / `dropoff_zone_id` are cast
straight through — location is *observed*, not inferred. (The Phase-2 point-in-polygon join that
rebuilt zones for the retired 2015-16 pack is superseded: this source has no coordinates at all.
The method stays documented in README.md §6 for history.)
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import duckdb

# distance bands used by the KPI (expected-duration banding) — assumption A4
DISTANCE_BANDS_SQL = """
  CASE
    WHEN dist <= 1.0 THEN 'a_0-1mi'
    WHEN dist <= 2.0 THEN 'b_1-2mi'
    WHEN dist <= 5.0 THEN 'c_2-5mi'
    WHEN dist <= 10.0 THEN 'd_5-10mi'
    WHEN dist <= 20.0 THEN 'e_10-20mi'
    ELSE 'f_20mi_plus'
  END"""

TIME_BUCKET_SQL = """
  CASE
    WHEN hour BETWEEN 0 AND 5   THEN 'overnight_00-06'
    WHEN hour BETWEEN 6 AND 9   THEN 'am_peak_06-10'
    WHEN hour BETWEEN 10 AND 15 THEN 'midday_10-16'
    WHEN hour BETWEEN 16 AND 19 THEN 'pm_peak_16-20'
    ELSE 'evening_20-24'
  END"""


def transform_month(month: str, staged_dir: Path, facts_dir: Path) -> dict:
    src = staged_dir / f"{month}.parquet"
    dst = facts_dir / f"{month}.parquet"
    facts_dir.mkdir(parents=True, exist_ok=True)

    sql = f"""
    COPY (
      SELECT
        '{month}' AS month,
        try_cast(tpep_pickup_datetime AS TIMESTAMP)  AS pickup_ts,
        try_cast(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
        epoch(try_cast(tpep_dropoff_datetime AS TIMESTAMP)
              - try_cast(tpep_pickup_datetime AS TIMESTAMP)) / 60.0 AS duration_min,
        try_cast(trip_distance AS DOUBLE) AS distance_mi,
        try_cast(passenger_count AS BIGINT) AS passenger_count,
        try_cast(ratecodeid AS BIGINT) AS ratecode_id,
        try_cast(payment_type AS BIGINT) AS payment_type,
        try_cast(vendorid AS BIGINT) AS vendor_id,
        upper(store_and_fwd_flag) AS store_and_fwd,
        try_cast(fare_amount AS DOUBLE) AS fare_amount,
        try_cast(tip_amount AS DOUBLE) AS tip_amount,
        try_cast(total_amount AS DOUBLE) AS total_amount,
        try_cast(airport_fee AS DOUBLE) AS airport_fee,
        try_cast(congestion_surcharge AS DOUBLE) AS congestion_surcharge,
        try_cast(pulocationid AS BIGINT) AS pickup_zone_id,
        try_cast(dolocationid AS BIGINT) AS dropoff_zone_id,
        date_part('hour', try_cast(tpep_pickup_datetime AS TIMESTAMP)) AS hour,
        date_part('dow',  try_cast(tpep_pickup_datetime AS TIMESTAMP)) AS day_of_week,
        TRY (try_cast(trip_distance AS DOUBLE)
             / (epoch(try_cast(tpep_dropoff_datetime AS TIMESTAMP)
                      - try_cast(tpep_pickup_datetime AS TIMESTAMP)) / 3600.0)) AS speed_mph,
        {DISTANCE_BANDS_SQL.replace('dist', "try_cast(trip_distance AS DOUBLE)")} AS distance_band,
        CASE
          WHEN date_part('dow', try_cast(tpep_pickup_datetime AS TIMESTAMP)) IN (0, 6) THEN true
          ELSE false
        END AS is_weekend,
        -- flag-rule outcomes travel with the row (flag != quarantine)
        r07_ok AS passenger_plausible,
        r08_ok AS money_non_negative,
        r09_ok AS total_matches_components,
        r10_ok AS ratecode_known,
        r13_ok AS payment_known,
        r14_ok AS pickup_zone_resolved,
        {TIME_BUCKET_SQL} AS time_bucket
      FROM read_parquet('{src}')
      WHERE fail_reasons IS NULL
    ) TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    t0 = time.time()
    duckdb.sql(sql)
    n = duckdb.sql(f"SELECT count(*) FROM read_parquet('{dst}')").fetchone()[0]
    return {"month": month, "fact_rows": int(n), "path": str(dst),
            "seconds": round(time.time() - t0, 1)}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Transform staged validated rows into trip facts")
    ap.add_argument("--staged", default="data/staged")
    ap.add_argument("--facts", default="data/facts")
    ap.add_argument("--months", nargs="*", help="subset (default: all staged files)")
    args = ap.parse_args(argv)

    staged_dir = Path(args.staged)
    facts_dir = Path(args.facts)
    files = sorted(staged_dir.glob("*.parquet"))
    if args.months:
        files = [p for p in files if p.stem in set(args.months)]
    if not files:
        print("no staged files found — run validate first")
        return 2

    for p in files:
        res = transform_month(p.stem, staged_dir, facts_dir)
        print(f"[transform] {res['month']}: {res['fact_rows']:,} fact rows ({res['seconds']}s)")
    print(f"facts → {facts_dir}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
