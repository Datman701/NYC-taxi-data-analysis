"""Stage 4 — METRICS: 5 operational metrics + the project KPI from trip facts.

Metric register (formula · grain · why · KPI link) lives in METRICS below and is
emitted with the numbers — the evidence table and the CSV carry the same text.

M1 valid_trip_rate        governance gate: can we publish at all?
M2 trip_time_reliability  PROJECT KPI: share of trips within expected duration
M3 trip_duration_typical  outcome detail: P50 / P90 minutes per month
M4 invalid_trip_rate      quality: share of raw rows quarantined (with components)
M5 slow_trip_share        driver: share of trips averaging < 6 mph (+ worst hour)

Zone slices (same ruler, different grain — not new metrics):
  metrics_zone_detail.csv  M2/M3/M5 per pickup zone (ranked when trips >= threshold)
  metrics_segments.csv     airport_zone vs city_zone vs zone_unknown per month
Zone ids are TLC's own (2023 source); 264/265 = Unknown/NA and land in zone_unknown.

Expected duration (A4, judgement call): P80 of duration per (distance_band, time_bucket)
computed on the BASELINE month; every month is judged against that same baseline.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd

METRICS = {
    "M1": {
        "name": "valid_trip_rate",
        "formula": "valid_rows / raw_rows (per month)",
        "grain": "month",
        "why": "If the validated population is too small, no KPI from it is publishable.",
        "kpi_link": "gating metric — KPI is only published while M1 ≥ 90%",
        "source": "output/validation_report.json",
    },
    "M2": {
        "name": "trip_time_reliability",
        "formula": "share of valid trips with duration_min <= expected_min(distance_band, time_bucket)"
                   " where expected_min = P80 on baseline month",
        "grain": "month",
        "why": "Direct measure of whether trips take as long as they should.",
        "kpi_link": "PROJECT KPI — this is the number leadership tracks",
        "source": "data/facts/*.parquet",
    },
    "M3": {
        "name": "trip_duration_typical",
        "formula": "P50 and P90 of duration_min (valid trips)",
        "grain": "month (detail by hour)",
        "why": "Median = typical rider experience; P90 = worst-experience tail.",
        "kpi_link": "outcome detail — explains moves in M2",
        "source": "data/facts/*.parquet",
    },
    "M4": {
        "name": "invalid_trip_rate",
        "formula": "quarantined_rows / raw_rows (per month); components: nonpositive duration,"
                   " missing/out-of-bbox GPS, distance out of range",
        "grain": "month",
        "why": "Data-quality cost of the pipeline; a rising rate means source problems.",
        "kpi_link": "supporting — protects M2's denominator, must be disclosed with M2",
        "source": "output/validation_report.json",
    },
    "M5": {
        "name": "slow_trip_share",
        "formula": "share of valid trips with avg speed < 6 mph; detail = same by pickup hour",
        "grain": "month (detail by hour)",
        "why": "Operational driver: congestion exposure, concentrated in specific hours.",
        "kpi_link": "driver metric — hours with high M5 have low M2",
        "source": "data/facts/*.parquet",
    },
}

SLOW_SPEED_MPH = 6.0


def _fact_columns(facts_dir: Path) -> set[str]:
    """Schema of the fact table (used to detect the Phase 2 zone columns)."""
    files = sorted(facts_dir.glob("*.parquet"))
    if not files:
        return set()
    import pyarrow.parquet as pq
    return set(pq.ParquetFile(files[0]).schema_arrow.names)


def build_expected_baseline(facts_dir: Path, baseline: str, out: Path) -> pd.DataFrame:
    exp = duckdb.sql(f"""
        SELECT distance_band, time_bucket,
               round(quantile_cont(duration_min, 0.80), 2) AS expected_duration_min,
               count(*) AS baseline_trips
        FROM read_parquet('{facts_dir}/{baseline}.parquet')
        GROUP BY 1, 2
        ORDER BY 1, 2          -- deterministic byte-identical reruns
    """).df()
    out.parent.mkdir(parents=True, exist_ok=True)
    exp.to_csv(out, index=False)
    return exp


def compute(out_dir: Path, facts_dir: Path, validation_report: Path,
            baseline: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    report = json.loads(validation_report.read_text())
    exp_path = out_dir / "expected_duration_baseline.csv"
    exp = build_expected_baseline(facts_dir, baseline, exp_path)

    # Scope the fact table to the months the validation report covers, so a stale file left
    # in data/facts can never leak into a published number.
    months = [f["month"] for f in report["files"]]
    month_filter = ", ".join(f"'{m}'" for m in months)
    facts = (f"(SELECT * FROM read_parquet('{facts_dir}/*.parquet') "
             f"WHERE month IN ({month_filter}))")
    exp_sql = f"read_csv_auto('{exp_path}')"

    # M2: reliability vs baseline expectation
    reliability = duckdb.sql(f"""
        SELECT f.month,
               avg(CASE WHEN f.duration_min <= e.expected_duration_min THEN 1.0 ELSE 0.0 END)
                 AS trip_time_reliability
        FROM {facts} f
        JOIN {exp_sql} e
          ON f.distance_band = e.distance_band AND f.time_bucket = e.time_bucket
        GROUP BY 1 ORDER BY 1
    """).df()

    # M3: P50 / P90 + M5: slow share + hourly detail
    monthly_dist = duckdb.sql(f"""
        SELECT month,
               round(quantile_cont(duration_min, 0.50), 2) AS duration_p50_min,
               round(quantile_cont(duration_min, 0.90), 2) AS duration_p90_min,
               avg(CASE WHEN speed_mph < {SLOW_SPEED_MPH} THEN 1.0 ELSE 0.0 END) AS slow_trip_share
        FROM {facts} GROUP BY 1 ORDER BY 1
    """).df()

    hourly = duckdb.sql(f"""
        SELECT f.month, f.hour,
               count(*) AS trips,
               round(quantile_cont(f.duration_min, 0.50), 2) AS duration_p50_min,
               round(quantile_cont(f.duration_min, 0.90), 2) AS duration_p90_min,
               avg(CASE WHEN f.speed_mph < {SLOW_SPEED_MPH} THEN 1.0 ELSE 0.0 END) AS slow_share,
               avg(CASE WHEN f.duration_min <= e.expected_duration_min THEN 1.0 ELSE 0.0 END)
                 AS trip_time_reliability
        FROM {facts} f
        JOIN {exp_sql} e
          ON f.distance_band = e.distance_band AND f.time_bucket = e.time_bucket
        GROUP BY 1, 2 ORDER BY 1, 2
    """).df()

    # M1 / M4 from the validation report (single source of truth for counts)
    m14 = pd.DataFrame([{
        "month": f["month"],
        "valid_trip_rate": f["coverage"],
        "invalid_trip_rate": round(1 - f["coverage"], 6),
        "raw_rows": f["total_rows"],
        "valid_rows": f["valid_rows"],
        "quarantined_rows": f["quarantined_rows"],
    } for f in report["files"]])

    merged = (m14.merge(monthly_dist, on="month").merge(reliability, on="month"))
    return merged, hourly


def to_long(monthly: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in monthly.iterrows():
        for mid, field in [("M1", "valid_trip_rate"), ("M2", "trip_time_reliability"),
                           ("M3", "duration_p50_min"), ("M3", "duration_p90_min"),
                           ("M4", "invalid_trip_rate"), ("M5", "slow_trip_share")]:
            m = METRICS[mid]
            rows.append({
                "month": r["month"],
                "metric_id": mid if field != "duration_p90_min" else "M3b",
                "metric_name": m["name"] + ("" if field != "duration_p90_min" else "_p90"),
                "metric_name_suffix": field.replace("_min", ""),
                "value": round(float(r[field]), 6),
                "grain": m["grain"],
                "formula": m["formula"],
                "kpi_link": m["kpi_link"],
            })
    out = pd.DataFrame(rows)
    # M3 appears twice (p50/p90): keep one formula text, distinguish by name
    out = out.drop(columns=["metric_name_suffix"])
    return out


def zone_outputs(out_dir: Path, facts_dir: Path, exp_path: Path,
                 lookup_csv: Path, airport_zone_ids: tuple[int, ...] = (1, 132),
                 min_zone_trips: int = 1000,
                 unresolved_zone_ids: tuple[int, ...] = (264, 265),
                 months: list[str] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Phase 2 slices: M2/M3/M5 per pickup zone + airport vs city segments.

    Same baseline ruler as the headline KPI (A4) — zones are compared with the
    same expected durations as the city, so numbers stay comparable.
    """
    months = months or []
    month_filter = ", ".join(f"'{m}'" for m in months) or "''"
    facts = (f"(SELECT * FROM read_parquet('{facts_dir}/*.parquet') "
             f"WHERE month IN ({month_filter}))")
    exp_sql = f"read_csv_auto('{exp_path}')"
    lookup = f"read_csv_auto('{lookup_csv}')"
    ap = ", ".join(str(i) for i in airport_zone_ids)
    max_real = unresolved_zone_ids[0] - 1        # 263: last real neighbourhood id

    zone = duckdb.sql(f"""
        SELECT f.month, f.pickup_zone_id,
               count(*) AS trips,
               round(quantile_cont(f.duration_min, 0.50), 2) AS duration_p50_min,
               round(quantile_cont(f.duration_min, 0.90), 2) AS duration_p90_min,
               avg(CASE WHEN f.speed_mph < {SLOW_SPEED_MPH} THEN 1.0 ELSE 0.0 END) AS slow_trip_share,
               avg(CASE WHEN f.duration_min <= e.expected_duration_min THEN 1.0 ELSE 0.0 END)
                 AS trip_time_reliability
        FROM {facts} f
        JOIN {exp_sql} e
          ON f.distance_band = e.distance_band AND f.time_bucket = e.time_bucket
        WHERE f.pickup_zone_id >= 1
        GROUP BY 1, 2 ORDER BY 1, 2
    """).df()
    zone = zone.merge(
        pd.read_csv(lookup_csv).rename(
            columns={"LocationID": "pickup_zone_id", "Zone": "pickup_zone",
                     "Borough": "borough"}),
        on="pickup_zone_id", how="left")
    zone["airport_zone"] = zone["pickup_zone_id"].isin(airport_zone_ids)
    zone["rankable"] = zone["trips"] >= min_zone_trips
    zone = zone[["month", "pickup_zone_id", "pickup_zone", "borough", "airport_zone",
                 "trips", "duration_p50_min", "duration_p90_min", "slow_trip_share",
                 "trip_time_reliability", "rankable"]]

    segment = duckdb.sql(f"""
        SELECT f.month,
               CASE WHEN f.pickup_zone_id IN ({ap})        THEN 'airport_zone'
                    WHEN f.pickup_zone_id BETWEEN 1 AND {max_real} THEN 'city_zone'
                    ELSE 'zone_unknown' END AS segment,
               count(*) AS trips,
               round(quantile_cont(f.duration_min, 0.50), 2) AS duration_p50_min,
               round(quantile_cont(f.duration_min, 0.90), 2) AS duration_p90_min,
               avg(CASE WHEN f.speed_mph < {SLOW_SPEED_MPH} THEN 1.0 ELSE 0.0 END) AS slow_trip_share,
               avg(CASE WHEN f.duration_min <= e.expected_duration_min THEN 1.0 ELSE 0.0 END)
                 AS trip_time_reliability
        FROM {facts} f
        JOIN {exp_sql} e
          ON f.distance_band = e.distance_band AND f.time_bucket = e.time_bucket
        GROUP BY 1, 2 ORDER BY 1, 2
    """).df()
    segment["share_of_trips"] = (segment["trips"] / segment.groupby("month")["trips"].transform("sum"))
    zone.to_csv(out_dir / "metrics_zone_detail.csv", index=False)
    segment.to_csv(out_dir / "metrics_segments.csv", index=False)
    return zone, segment


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Compute the 5 metrics + KPI")
    ap.add_argument("--facts", default="data/facts")
    ap.add_argument("--out", default="output")
    ap.add_argument("--validation-report", default="output/validation_report.json")
    ap.add_argument("--baseline", default="2023-01",
                    help="baseline month for expected durations (A4)")
    ap.add_argument("--zone-lookup", default="data/reference/taxi_zone_lookup.csv")
    args = ap.parse_args(argv)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    report = json.loads(Path(args.validation_report).read_text())
    validated_months = [f["month"] for f in report["files"]]
    monthly, hourly = compute(out_dir, Path(args.facts),
                              Path(args.validation_report), args.baseline)

    long = to_long(monthly)
    long.to_csv(out_dir / "metrics_monthly.csv", index=False)
    hourly.to_csv(out_dir / "metrics_hourly_detail.csv", index=False)
    monthly.to_csv(out_dir / "metrics_monthly_wide.csv", index=False)

    lookup = Path(args.zone_lookup)
    if (Path(args.facts)).exists() and lookup.exists() \
            and "pickup_zone_id" in _fact_columns(Path(args.facts)):
        zone, segment = zone_outputs(out_dir, Path(args.facts),
                                     out_dir / "expected_duration_baseline.csv", lookup,
                                     months=validated_months)
        print(f"zone detail → {out_dir}/metrics_zone_detail.csv "
              f"({len(zone)} month-zone rows)")
        print(f"segments    → {out_dir}/metrics_segments.csv")

    print(f"baseline for expected durations: {args.baseline} (P80 per distance band × time bucket)")
    print(monthly.to_string(index=False))
    print(f"\nmetrics → {out_dir}/metrics_monthly.csv")
    print(f"hourly  → {out_dir}/metrics_hourly_detail.csv")
    print(f"baseline→ {out_dir}/expected_duration_baseline.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
