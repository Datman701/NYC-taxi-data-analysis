"""Validation rules — the executable validation contract (2023 source).

The prose version lives in README.md §5 *Validation contract*; every threshold and its
observed 2023 rate is in config.yaml.

Conventions
-----------
- Raw parquet is read with TRY_CAST in every predicate, so a malformed value can never
  abort the job — it becomes a *counted failure* instead.
- A predicate returns TRUE when the row PASSES the rule.
- `severity`:
    QUARANTINE -> row fails = trip leaves the KPI population (fail_reasons set)
    FLAG       -> row fails = counted & reported, row stays in population (needs owner)
- `fixed_status` -> rule has no row predicate (UNKNOWN / limitation entries).

2023 source specifics (all measured on the 38.3M-row 2023 retrieval):
  * no coordinates at all -> location is checked with TLC's own zone ids (R04/R05)
  * `airport_fee` + `congestion_surcharge` join the money components (R09)
  * `vendorid = 6` exists (8,668 rows) and is not in the TLC user guide (U4)
  * 3.4% of rows have no rate code / payment type / passenger count (U5)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

QUARANTINE = "QUARANTINE"
FLAG = "FLAG"


@dataclass(frozen=True)
class Rule:
    id: str
    name: str
    category: str                 # technical | semantic | organizational
    severity: str                 # QUARANTINE | FLAG | REPORT
    description: str
    owner: str
    action: str                   # what we do when it fires
    predicate: Callable[[dict], str] | None = None   # TRUE = row passes; None = fixed status
    fixed_status: str | None = None                  # e.g. UNKNOWN for semantic entries
    evidence: str | None = None                      # for fixed-status rules

    def sql(self, cfg: dict) -> str:
        assert self.predicate is not None, self.id
        return self.predicate(cfg)


def _pu() -> str:
    return "try_cast(tpep_pickup_datetime AS TIMESTAMP)"


def _do() -> str:
    return "try_cast(tpep_dropoff_datetime AS TIMESTAMP)"


def _num(col: str) -> str:
    return f"try_cast({col} AS DOUBLE)"


def _int(col: str) -> str:
    return f"try_cast({col} AS BIGINT)"


def _rules() -> list[Rule]:
    return [
        Rule(
            id="R00", name="timestamps_parseable", category="technical", severity=QUARANTINE,
            description="pickup & dropoff parse as TIMESTAMP",
            owner="FDE (technical contract)",
            action="quarantine row — a trip with unreadable time has no duration",
            predicate=lambda c: f"{_pu()} IS NOT NULL AND {_do()} IS NOT NULL",
        ),
        Rule(
            id="R01", name="dropoff_after_pickup", category="technical", severity=QUARANTINE,
            description="dropoff > pickup (strictly positive duration)",
            owner="FDE (technical contract)",
            action="quarantine row — zero/negative duration cannot enter duration metrics",
            predicate=lambda c: f"{_do()} > {_pu()}",
        ),
        Rule(
            id="R02", name="duration_within_24h", category="technical", severity=QUARANTINE,
            description="duration <= limits.duration_max_seconds (default 24h)",
            owner="CLIENT — threshold is a judgement call (assumption A3)",
            action="quarantine row — longer trips treated as meter errors until client confirms",
            predicate=lambda c: (
                f"{_do()} <= {_pu()} + INTERVAL '{c['limits']['duration_max_seconds']} seconds'"),
        ),
        Rule(
            id="R03", name="pickup_in_file_month", category="technical", severity=QUARANTINE,
            description="pickup timestamp falls in the month the file claims (partition integrity)",
            owner="FDE (partition contract)",
            action="quarantine row — otherwise monthly metrics double-count / mis-month",
            predicate=lambda c: f"date_trunc('month', {_pu()}) = DATE '{c['month']}-01'",
        ),
        Rule(
            id="R04", name="pickup_zone_known", category="technical", severity=QUARANTINE,
            description="pulocationid present and a real TLC zone id (1..265)",
            owner="FDE (zone contract, ids from the TLC zone lookup)",
            action="quarantine row — the KPI's location dimension would be undefined",
            predicate=lambda c: f"{_int('pulocationid')} BETWEEN 1 AND {c['zones']['max_zone_id']}",
        ),
        Rule(
            id="R05", name="dropoff_zone_known", category="technical", severity=QUARANTINE,
            description="dolocationid present and a real TLC zone id (1..265)",
            owner="FDE (zone contract)",
            action="quarantine row — same as R04 for the dropoff end",
            predicate=lambda c: f"{_int('dolocationid')} BETWEEN 1 AND {c['zones']['max_zone_id']}",
        ),
        Rule(
            id="R06", name="distance_in_range", category="technical", severity=QUARANTINE,
            description="0 < trip_distance <= limits.distance_max_miles",
            owner="FDE (KPI is distance-banded: a trip without distance has no expected duration)",
            action="quarantine row — zero/negative/absurd distance breaks the KPI banding",
            predicate=lambda c: (
                f"{_num('trip_distance')} > 0 AND {_num('trip_distance')} <= {c['limits']['distance_max_miles']}"
            ),
        ),
        Rule(
            id="R07", name="passenger_count_plausible", category="semantic", severity=FLAG,
            description="passenger_count in 1..6",
            owner="CLIENT (unknown = U2)",
            action="flag only — 0/7/8/9 kept in population, reported for owner confirmation",
            predicate=lambda c: f"{_int('passenger_count')} BETWEEN 1 AND 6",
        ),
        Rule(
            id="R08", name="money_non_negative", category="technical", severity=FLAG,
            description="fare_amount >= 0 and total_amount >= 0",
            owner="FDE / finance",
            action="flag only — negatives are fare corrections/chargebacks; money is not in the KPI",
            predicate=lambda c: (
                f"{_num('fare_amount')} >= 0 AND {_num('total_amount')} >= 0"
            ),
        ),
        Rule(
            id="R09", name="total_matches_components", category="technical", severity=FLAG,
            description=("total_amount vs fare+extra+mta_tax+tip+tolls+improvement_surcharge"
                         "+airport_fee+congestion_surcharge within limits.money_tolerance_usd"
                         " (absolute; calibrated on 2023 — 0% of rows exceed $5, 3.3% exceed $2.50)"),
            owner="finance",
            action=("flag only — airport/congestion fee inclusion conventions differ per row; rows"
                    " kept. Missing surcharge columns are read as 0 (A7); those rows are still"
                    " disclosed by R10/R11/R13"),
            predicate=lambda c: (
                f"abs({_num('total_amount')} - ({_num('fare_amount')} + {_num('extra')} + "
                f"{_num('mta_tax')} + {_num('tip_amount')} + {_num('tolls_amount')} + "
                f"{_num('improvement_surcharge')} + coalesce({_num('airport_fee')}, 0) + "
                f"coalesce({_num('congestion_surcharge')}, 0))) "
                f"<= {c['limits']['money_tolerance_usd']}"
            ),
        ),
        Rule(
            id="R10", name="ratecode_known", category="semantic", severity=FLAG,
            description="ratecodeid in 1..6 (99 undefined per TLC guide = U1; null = U5)",
            owner="CLIENT (U1, U5)",
            action="flag only — 99 and nulls kept, need owner confirmation; rate code is not in the KPI",
            predicate=lambda c: f"{_int('ratecodeid')} IN (1,2,3,4,5,6)",
        ),
        Rule(
            id="R11", name="store_and_fwd_known", category="semantic", severity=FLAG,
            description="store_and_fwd_flag in {Y,N}",
            owner="CLIENT",
            action="flag only — representation check (nulls belong to the U5 cohort)",
            predicate=lambda c: "upper(store_and_fwd_flag) IN ('Y','N')",
        ),
        Rule(
            id="R12", name="vendor_known", category="technical", severity=FLAG,
            description="vendorid in the observed set {1,2,6} (6 undocumented = U4)",
            owner="FDE / TLC (U4)",
            action="flag only — new vendor id accepted for now, reported for confirmation",
            predicate=lambda c: (
                f"{_int('vendorid')} IN ({', '.join(str(v) for v in c['zones']['known_vendor_ids'])})"
            ),
        ),
        Rule(
            id="R13", name="payment_type_known", category="semantic", severity=FLAG,
            description="payment_type in 1..6 (0 = unknown, part of the U5 cohort)",
            owner="CLIENT (U5)",
            action="flag only — payment mix is descriptive, not in the KPI",
            predicate=lambda c: f"{_int('payment_type')} BETWEEN 1 AND 6",
        ),
        Rule(
            id="R14", name="pickup_zone_resolved", category="semantic", severity=FLAG,
            description="pickup zone is a real neighbourhood (not 264 Unknown / 265 NA)",
            owner="FDE (disclosure)",
            action="flag only — trips located 'Unknown' stay in the population but are disclosed",
            predicate=lambda c: (
                f"{_int('pulocationid')} NOT IN ({', '.join(str(v) for v in c['zones']['unresolved_zone_ids'])})"
            ),
        ),
        Rule(
            id="R15", name="dropoff_zone_resolved", category="semantic", severity=FLAG,
            description="dropoff zone is a real neighbourhood (not 264/265)",
            owner="FDE (disclosure)",
            action="flag only — same as R14 for the dropoff end",
            predicate=lambda c: (
                f"{_int('dolocationid')} NOT IN ({', '.join(str(v) for v in c['zones']['unresolved_zone_ids'])})"
            ),
        ),
        # ---- fixed-status entries (no row predicate; recorded in the report) ----
        Rule(
            id="U1", name="ratecode_99_undefined", category="semantic", severity="REPORT",
            description="Meaning of ratecodeid = 99 is absent from the TLC user guide",
            owner="CLIENT / TLC research@tlc.nyc.gov",
            action="UNKNOWN — cannot encode a rule without owner definition",
            fixed_status="UNKNOWN",
            evidence="213,480 rows (0.56%) in 2023; detected as failures of R10.",
        ),
        Rule(
            id="U2", name="passenger_zero_or_high", category="semantic", severity="REPORT",
            description="passenger_count 0 / 7 / 8 / 9 — unknown vs error vs courier",
            owner="CLIENT",
            action="UNKNOWN — kept in population, flagged by R07",
            fixed_status="UNKNOWN",
            evidence="583,414 rows (1.52%) in 2023; needs client definition.",
        ),
        Rule(
            id="U3", name="zero_distance_positive_fare", category="semantic", severity="REPORT",
            description="Zero trip_distance with positive fare — meter fault, repositioning or GPS fault?",
            owner="CLIENT",
            action="UNKNOWN — quarantined by R06 for KPI safety, cause unresolved",
            fixed_status="UNKNOWN",
            evidence="773,445 rows (2.02%) in 2023, rising to 3.4% by Sep-Nov; subset of R06 failures.",
        ),
        Rule(
            id="U4", name="vendor_id_6_undocumented", category="semantic", severity="REPORT",
            description="vendorid = 6 appears in 2023 but is not in the TLC user guide",
            owner="CLIENT / TLC",
            action="UNKNOWN — accepted in R12 for now, flagged for owner confirmation",
            fixed_status="UNKNOWN",
            evidence="8,668 rows (0.02%) in 2023.",
        ),
        Rule(
            id="U5", name="missing_trip_metadata_cohort", category="semantic", severity="REPORT",
            description=("3.4% of 2023 rows have no ratecodeid, no passenger_count, no"
                         " store_and_fwd_flag and payment_type = 0 — real trips with missing metadata?"),
            owner="CLIENT / TLC",
            action="UNKNOWN — rows kept (their times, distance and duration are valid), flagged by R10/R11/R13",
            fixed_status="UNKNOWN",
            evidence="1,309,356 rows; p50 duration 16.6 min, avg distance 18.5 mi — looks like real trips.",
        ),
        Rule(
            id="L1", name="meter_accuracy_disclaimer", category="organizational", severity="REPORT",
            description="TLC did not create the data and disclaims accuracy",
            owner="TLC (published limitation)",
            action="LIMITATION — recorded, cannot be fixed by validation",
            fixed_status="UNKNOWN",
            evidence="TLC trip-record page: 'The trip data was not created by the TLC…'",
        ),
    ]


def all_rules() -> list[Rule]:
    return _rules()


def quarantine_rules(cfg: dict) -> list[Rule]:
    ids = set(cfg["quarantine"])
    return [r for r in all_rules() if r.id in ids]


def flag_rules(cfg: dict) -> list[Rule]:
    ids = set(cfg["quarantine"])
    return [r for r in all_rules() if r.predicate is not None and r.id not in ids]


# 2023 Socrata schema (19 columns). Longitude/latitude are NOT part of this source, and
# `imp_surcharge` (a 2015-16 column) is gone — 2023 reports `improvement_surcharge` instead.
CANONICAL_COLUMNS = [
    "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "ratecodeid", "store_and_fwd_flag", "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "airport_fee", "congestion_surcharge",
    "total_amount", "pulocationid", "dolocationid",
]
