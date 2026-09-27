"""Crafted 2023 fixture — one row per rule behaviour, 19-column TLC/Socrata schema.

Row 0 is fully valid. Rows 1-7 + 15 break exactly one QUARANTINE rule each, rows 8-13 break
one FLAG rule each, and row 14 has unparseable timestamps. Values are stored as strings on
purpose: predicates use TRY_CAST, so a dirty value must be *counted*, never crash the job.
"""

import pyarrow as pa
import pyarrow.parquet as pq

COLUMNS = [
    "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "ratecodeid", "store_and_fwd_flag", "payment_type", "fare_amount",
    "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "airport_fee", "congestion_surcharge", "total_amount",
    "pulocationid", "dolocationid",
]

# 2023-01-10 08:00 -> 08:20, JFK (132) -> Brooklyn (141), $18.35 collected
GOOD = ["2", "2023-01-10 08:00:00", "2023-01-10 08:20:00", "1", "3.50", "1", "N", "1",
        "14", "0.50", "0.50", "3.05", "0", "1.00", "0", "0", "18.35", "132", "141"]


def _row(pu, doff, passenger="1", dist="3.50", rate="1", sfw="N", pay="1", fare="14",
         total="18.35", pu_zone="132", do_zone="141") -> list[str]:
    """Build a 19-value row with sane money components unless overridden."""
    return ["2", pu, doff, passenger, dist, rate, sfw, pay, fare,
            "0.50", "0.50", "3.05", "0", "1.00", "0", "0", total, pu_zone, do_zone]


ROWS = [
    GOOD,                                                              # 0  fully valid
    _row("2023-01-10 09:00:00", "2023-01-10 09:00:00", dist="1.20", fare="7", total="8.30"),   # 1  R01
    _row("2023-01-10 10:00:00", "2023-01-10 09:00:00", dist="1.20", fare="7", total="8.30"),   # 2  R01
    _row("2023-01-09 08:00:00", "2023-01-11 08:00:00"),                 # 3  R02 (>24h)
    _row("2023-02-10 08:00:00", "2023-02-10 08:20:00"),                 # 4  R03 (wrong month)
    _row("2023-01-10 11:00:00", "2023-01-10 11:20:00", pu_zone="999"),  # 5  R04
    _row("2023-01-10 12:00:00", "2023-01-10 12:20:00", do_zone="999"),  # 6  R05
    _row("2023-01-10 13:00:00", "2023-01-10 13:20:00", dist="0"),      # 7  R06
    _row("2023-01-10 14:00:00", "2023-01-10 14:20:00", passenger="9"),  # 8  R07
    _row("2023-01-10 15:00:00", "2023-01-10 15:20:00", fare="-6", total="-4.20"),  # 9  R08
    _row("2023-01-10 16:00:00", "2023-01-10 16:20:00", total="30.00"),  # 10 R09 (off by >$5)
    _row("2023-01-10 17:00:00", "2023-01-10 17:20:00", rate="99"),      # 11 R10
    _row("2023-01-10 18:00:00", "2023-01-10 18:20:00", sfw="Q"),        # 12 R11
    _row("2023-01-10 19:00:00", "2023-01-10 19:20:00", pu_zone="264"),  # 13 R14 (Unknown)
    ["2", "not-a-timestamp", "also-not-a-timestamp", "1", "3.50", "1", "N", "1",      # 14 R00
     "14", "0.50", "0.50", "3.05", "0", "1.00", "0", "0", "18.35", "132", "141"],
    ["6", "2023-01-10 20:00:00", "2023-01-10 20:20:00", "2", "500.00", "2", "Y", "2",   # 15 R06
     "300", "0", "0", "0", "0", "1.00", "0", "0", "301.00", "132", "1"],
]

assert all(len(r) == len(COLUMNS) for r in ROWS), [len(r) for r in ROWS]


def write_fixture(path) -> None:
    """Write the crafted rows to a parquet file (all columns VARCHAR on purpose)."""
    table = pa.table({c: pa.array([r[i] for r in ROWS], pa.string())
                      for i, c in enumerate(COLUMNS)})
    pq.write_table(table, str(path), compression="zstd")


def write_sample_month(path, months=("2023-01", "2023-02")) -> None:
    """A slightly larger deterministic sample used by the end-to-end smoke test."""
    rows = []
    for m in months:
        for k in range(60):
            hh = f"{8 + k % 10:02d}"
            rows.append([
                "2", f"{m}-10 {hh}:00:00", f"{m}-10 {hh}:30:00", str(1 + k % 4),
                f"{1 + (k % 40) / 4:.2f}", "1", "N", "1",
                f"{10 + k % 20}.00", "0.50", "0.50", "3.00", "0", "1.00", "0", "0",
                f"{15 + k % 20}.80", str(1 + k % 263), str(1 + (k * 7) % 263),
            ])
    table = pa.table({c: pa.array([r[i] for r in rows], pa.string())
                      for i, c in enumerate(COLUMNS)})
    pq.write_table(table, str(path), compression="zstd")
