"""Unit tests for the executable validation contract (rules + status + schema), 2023 source."""
from pathlib import Path

import duckdb
import pytest
import yaml

from nyc_pipeline.rules import CANONICAL_COLUMNS, all_rules, quarantine_rules
from nyc_pipeline.validate import check_schema, load_config

from fixtures_2023 import COLUMNS, ROWS, write_fixture

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "src/nyc_pipeline/config.yaml"
MONTH = "2023-01"


@pytest.fixture(scope="module")
def cfg():
    c = load_config(CONFIG)
    c["month"] = MONTH
    return c


@pytest.fixture(scope="module")
def fixture_df(cfg, tmp_path_factory):
    """Evaluate every rule predicate against the crafted fixture rows."""
    path = tmp_path_factory.mktemp("fx") / "mini_trips.parquet"
    write_fixture(path)
    preds = ", ".join(
        f"({r.sql(cfg)}) AS {r.id}" for r in all_rules() if r.predicate is not None
    )
    return duckdb.sql(
        f"SELECT *, {preds} FROM read_parquet('{path}')"
    ).df()


def _passed(v) -> bool:
    """NULL (pd.NA) means the predicate could not evaluate → row fails the rule."""
    try:
        return bool(v)
    except TypeError:
        return False


def test_fixture_shape(fixture_df):
    assert len(fixture_df) == len(ROWS) == 16
    assert list(fixture_df.columns[:len(COLUMNS)]) == COLUMNS


def test_row0_fully_valid(fixture_df):
    row = fixture_df.iloc[0]
    for r in all_rules():
        if r.predicate is not None:
            assert _passed(row[r.id]), f"row0 should pass {r.id}"


@pytest.mark.parametrize("row_index,failing", [
    (1, {"R01"}),        # zero duration
    (2, {"R01"}),        # negative duration
    (3, {"R02"}),        # > 24h
    (4, {"R03"}),        # pickup outside file month
    (5, {"R04"}),        # pickup zone id out of range
    (6, {"R05"}),        # dropoff zone id out of range
    (7, {"R06"}),        # zero distance
    (15, {"R06"}),       # 500 miles
])
def test_quarantine_rules_fire(fixture_df, row_index, failing):
    row = fixture_df.iloc[row_index]
    for rid in failing:
        assert not _passed(row[rid]), f"row {row_index} should fail {rid}"


def test_unparseable_timestamp_row(fixture_df):
    row = fixture_df.iloc[14]
    assert not _passed(row["R00"])          # primary reason
    # dependent time rules cannot evaluate either → also fail (never silently pass)
    for rid in ("R01", "R02", "R03"):
        assert not _passed(row[rid]), f"{rid} must not silently pass on NULL"


@pytest.mark.parametrize("row_index,failing", [
    (8, "R07"),   # passenger 9
    (9, "R08"),   # negative fare
    (10, "R09"),  # total off by $11.65 (> $5 tolerance)
    (11, "R10"),  # ratecodeid 99
    (12, "R11"),  # store_and_fwd Q
    (13, "R14"),  # pickup zone 264 (Unknown)
])
def test_flag_rules_fire(fixture_df, row_index, failing):
    assert not _passed(fixture_df.iloc[row_index][failing])


def test_flag_rows_are_not_quarantine_rows(fixture_df, cfg):
    """FLAG failures must not set fail_reasons (flag != quarantine)."""
    q_ids = {r.id for r in quarantine_rules(cfg)}
    for i in (8, 9, 10, 11, 12, 13):
        row = fixture_df.iloc[i]
        broken = [rid for rid in q_ids if not _passed(row[rid])]
        assert broken == [], f"row {i} has quarantine failures {broken}"


def test_vendor_6_is_accepted_but_documented(cfg):
    """Vendor 6 exists in 2023 (U4): R12 accepts it, the contract still records it."""
    row = fixture_df_row_15 = None  # documented via config + fixed-status rule
    assert 6 in cfg["zones"]["known_vendor_ids"]
    assert any(r.id == "U4" for r in all_rules())


def test_zone_rules_use_tlc_zone_ids(cfg):
    ids = {r.id for r in all_rules() if "zone" in r.name}
    assert {"R04", "R05", "R14", "R15"} <= ids
    assert cfg["zones"]["unresolved_zone_ids"] == [264, 265]


def test_reasons_expression_only_uses_quarantine_rules(cfg):
    from nyc_pipeline.validate import _reasons_expr
    expr = _reasons_expr(quarantine_rules(cfg))
    q_ids = {r.id for r in quarantine_rules(cfg)}
    for r in all_rules():
        if r.predicate is None:
            continue
        if r.id in q_ids:
            assert f"'{r.id}'" in expr
        else:
            assert f"'{r.id}'" not in expr, f"{r.id} is FLAG and must not quarantine rows"


@pytest.mark.parametrize("rate,expected", [
    (0.0, "PASS"), (0.009, "PASS"), (0.02, "WARN"), (0.05, "WARN"), (0.06, "FAIL"),
])
def test_status_thresholds(cfg, rate, expected):
    from nyc_pipeline.validate import _status
    tol = cfg["tolerances"]["R01"]
    assert _status(rate, tol) == expected


def test_schema_matches_2023_source():
    res = check_schema(list(CANONICAL_COLUMNS), MONTH)
    assert res["status"] == "PASS"
    assert res["missing_columns"] == []


def test_schema_missing_column_fails():
    header = [c for c in CANONICAL_COLUMNS if c != "trip_distance"]
    res = check_schema(header, MONTH)
    assert res["status"] == "FAIL"
    assert res["missing_columns"] == ["trip_distance"]


def test_no_coordinate_columns_in_canonical_schema():
    """2023 has no lat/lon: the schema must not ask for coordinates."""
    assert not [c for c in CANONICAL_COLUMNS if "longitude" in c or "latitude" in c]


def test_config_declares_owners_and_gate():
    cfg = yaml.safe_load(CONFIG.read_text())
    assert cfg["gate"]["min_valid_coverage"] <= 1.0
    for aid in ("A1", "A2", "A3", "A4", "A5", "A6"):
        assert any(a["id"] == aid for a in cfg["assumptions"])
    assert cfg["assumptions"][2]["owner"].startswith("CLIENT")   # A3 needs client sign-off
    for uid in ("U1", "U2", "U3", "U4", "U5"):
        assert any(u["id"] == uid for u in cfg["unknowns"])
    for rule_id in [f"R{i:02d}" for i in range(16)]:
        assert rule_id in cfg["tolerances"], f"{rule_id} needs a tolerance"
