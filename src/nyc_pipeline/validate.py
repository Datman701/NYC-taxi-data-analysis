"""Stage 3 — VALIDATE: run the executable validation contract over the raw parquet.

Inputs are the API-retrieved per-month parquet files (fingerprinted in the manifest); the raw
page payloads stay untouched in `data/api_raw/`.

Outputs (nothing is ever dropped):
  data/staged/<month>.parquet        ALL rows + per-rule booleans + fail_reasons
  data/quarantine/<month>.parquet    rows with >= 1 QUARANTINE failure (+reason codes)
  output/validation_report.json      PASS/WARN/FAIL/UNKNOWN per rule + PUBLISH/HOLD gate

Job-level aborts (exit 3): schema mismatch, row-conservation break.
Gate HOLD (exit 0): report is produced but marked HOLD for the publish decision.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml

from .rules import CANONICAL_COLUMNS, Rule, all_rules, flag_rules, quarantine_rules

ABORT = 3


def load_config(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def check_schema(header: list[str], month: str) -> dict:
    """Case-insensitive conformance to the canonical schema (drift = representation)."""
    have = {h.lower() for h in header}
    want = {c.lower() for c in CANONICAL_COLUMNS}
    missing = sorted(want - have)
    extra = sorted(have - want)
    return {
        "month": month,
        "status": "PASS" if not missing else "FAIL",
        "missing_columns": missing,
        "unexpected_columns": extra,           # recorded, not fatal
        "header": list(header),
    }


def _predicates(cfg: dict, rules: list[Rule]) -> str:
    return ",\n      ".join(f"({r.sql(cfg)}) AS {r.id.lower()}_ok" for r in rules)


def _reasons_expr(rules: list[Rule]) -> str:
    """Quarantine reason codes.

    COALESCE(ok, false) is essential: a predicate over a NULL value (e.g. `ratecodeid IN (...)`
    where the rate code is missing) evaluates to NULL, and `NOT NULL` is NULL — which would
    silently treat the row as a pass. A rule that cannot be evaluated counts as failed.
    """
    cases = ", ".join(
        f"CASE WHEN COALESCE({r.id.lower()}_ok, false) THEN NULL ELSE '{r.id}' END" for r in rules
    )
    return f"nullif(concat_ws(';', {cases}), '')"


def validate_file(path: Path, month: str, cfg: dict, staged_dir: Path,
                  quarantine_dir: Path) -> dict:
    cfg_m = dict(cfg)
    cfg_m["month"] = month
    pred_rules = [r for r in all_rules() if r.predicate is not None]
    q_rules = quarantine_rules(cfg_m)

    staged_path = staged_dir / f"{month}.parquet"
    q_path = quarantine_dir / f"{month}.parquet"

    copy_sql = f"""
    COPY (
      SELECT *,
        {_reasons_expr(q_rules)} AS fail_reasons
      FROM (
        SELECT *,
          {_predicates(cfg_m, pred_rules)}
        FROM read_parquet('{path}')
      )
    ) TO '{staged_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    t0 = time.time()
    duckdb.sql(copy_sql)
    duckdb.sql(f"""
      COPY (SELECT * FROM read_parquet('{staged_path}') WHERE fail_reasons IS NOT NULL)
      TO '{q_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    sums = ",\n".join(
        f"sum(CASE WHEN COALESCE({r.id.lower()}_ok, false) THEN 0 ELSE 1 END) AS {r.id}_fails"
        for r in pred_rules
    )
    row = duckdb.sql(f"""
      SELECT count(*) AS total,
        sum(CASE WHEN fail_reasons IS NULL THEN 1 ELSE 0 END) AS valid,
        {sums}
      FROM read_parquet('{staged_path}')
    """).fetchone()
    names = ["total", "valid"] + [f"{r.id}_fails" for r in pred_rules]
    stats = dict(zip(names, row))
    stats = {k: int(v or 0) for k, v in stats.items()}

    return {
        "month": month,
        "raw_path": str(path),
        "staged_path": str(staged_path),
        "quarantine_path": str(q_path),
        "total_rows": stats["total"],
        "valid_rows": stats["valid"],
        "quarantined_rows": stats["total"] - stats["valid"],
        "coverage": round(stats["valid"] / stats["total"], 6) if stats["total"] else 0.0,
        "rule_fails": {r.id: stats[f"{r.id}_fails"] for r in pred_rules},
        "seconds": round(time.time() - t0, 1),
    }


def _status(rate: float, tol: dict) -> str:
    if rate > tol["fail"]:
        return "FAIL"
    if rate > tol["warn"]:
        return "WARN"
    return "PASS"


def _integrity_checks(files: list[dict], manifest_files: list[dict]) -> list[dict]:
    """Upstream retrieval integrity, carried into the validation report.

    * api_count_reconciled — the month was proven complete against the API's own COUNT(*)
      during retrieval (api_fetch report), not merely read from disk.
    * composite_key_duplicates — offset paging could in principle repeat or skip rows if the
      server-side ORDER BY were unstable; a duplicate count near zero is the evidence it wasn't.
    """
    by_month = {f["month"]: f for f in manifest_files}
    checks = []
    for f in files:
        ret = (by_month.get(f["month"], {}).get("retrieval") or {})
        dupes = ret.get("composite_key_duplicates")
        checks.append({
            "month": f["month"],
            "api_count": ret.get("api_count"),
            "rows_staged": f["total_rows"],
            "api_count_reconciled": ret.get("count_reconciled"),
            "composite_key_duplicates": dupes,
            "duplicate_rate": (round(dupes / f["total_rows"], 8)
                               if dupes is not None and f["total_rows"] else None),
            "ok": ret.get("count_reconciled") is True and (dupes or 0) <= max(
                1, int(0.0001 * f["total_rows"])),
        })
    return checks


def build_report(file_results: list[dict], schema_results: list[dict], cfg: dict,
                 manifest_rows: dict[str, int], manifest_files: list[dict] | None = None) -> dict:
    pred_rules = [r for r in all_rules() if r.predicate is not None]
    total_rows = sum(f["total_rows"] for f in file_results)
    total_valid = sum(f["valid_rows"] for f in file_results)

    rules_out = []
    for r in pred_rules:
        fails = sum(f["rule_fails"][r.id] for f in file_results)
        rate = fails / total_rows if total_rows else 0.0
        tol = cfg["tolerances"][r.id]
        rules_out.append({
            "id": r.id, "name": r.name, "category": r.category, "severity": r.severity,
            "description": r.description, "owner": r.owner, "action": r.action,
            "failures": fails, "fail_rate": round(rate, 6),
            "tolerances": tol, "status": _status(rate, tol),
        })
    for r in all_rules():
        if r.predicate is None:
            rules_out.append({
                "id": r.id, "name": r.name, "category": r.category, "severity": r.severity,
                "description": r.description, "owner": r.owner, "action": r.action,
                "failures": None, "fail_rate": None, "tolerances": None,
                "status": r.fixed_status, "evidence": r.evidence,
            })

    # row conservation: staged == manifest raw rows (per file)
    conservation = []
    for f in file_results:
        expected = manifest_rows.get(f["month"])
        conservation.append({
            "month": f["month"], "manifest_rows": expected, "staged_rows": f["total_rows"],
            "quarantined": f["quarantined_rows"],
            "ok": expected is not None and f["total_rows"] == expected,
        })
    conservation_ok = all(c["ok"] for c in conservation)
    schema_ok = all(s["status"] == "PASS" for s in schema_results)
    integrity = _integrity_checks(file_results, manifest_files or [])
    integrity_ok = all(c["ok"] for c in integrity)

    coverage = total_valid / total_rows if total_rows else 0.0
    gate_reasons = []
    if not schema_ok:
        gate_reasons.append("schema conformance FAIL")
    if not conservation_ok:
        gate_reasons.append("row conservation broken (staged != manifest)")
    if integrity and not integrity_ok:
        gate_reasons.append("retrieval integrity check failed (API reconciliation or duplicates)")
    if coverage < cfg["gate"]["min_valid_coverage"]:
        gate_reasons.append(
            f"valid coverage {coverage:.3f} < min {cfg['gate']['min_valid_coverage']}")
    for r in rules_out:
        if r["status"] == "FAIL":
            gate_reasons.append(f"rule {r['id']} ({r['name']}) FAIL "
                                f"rate={r['fail_rate']:.4f}")

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "kpi": cfg["project"]["kpi"],
        "decision": cfg["project"]["decision"],
        "totals": {
            "raw_rows": total_rows, "valid_rows": total_valid,
            "quarantined_rows": total_rows - total_valid,
            "coverage": round(coverage, 6),
        },
        "gate": {
            "status": cfg["gate"]["hold_label"] if gate_reasons else cfg["gate"]["publish_label"],
            "reasons": gate_reasons,
            "min_valid_coverage": cfg["gate"]["min_valid_coverage"],
        },
        "schema": schema_results,
        "row_conservation": conservation,
        "integrity_checks": integrity,
        "files": file_results,
        "rules": rules_out,
        "assumptions": cfg.get("assumptions", []),
        "limitations": cfg.get("limitations", []),
        "unknowns": cfg.get("unknowns", []),
    }


def print_gate(report: dict) -> None:
    print("\n=== RULE GATE ===")
    for r in report["rules"]:
        rate = "     -" if r["fail_rate"] is None else f"{r['fail_rate']*100:6.3f}%"
        n = "-" if r["failures"] is None else f"{r['failures']:,}"
        print(f"{r['id']:4} {r['severity']:10} {r['status']:8} fails={n:>12} rate={rate}  {r['name']}")
    g = report["gate"]
    print(f"\ncoverage={report['totals']['coverage']:.4f}  "
          f"valid={report['totals']['valid_rows']:,}/{report['totals']['raw_rows']:,}")
    print(f"GATE: {g['status']}" + ("" if not g["reasons"] else f"  ← {'; '.join(g['reasons'])}"))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Validate raw TLC parquet against the contract")
    ap.add_argument("--config", default="src/nyc_pipeline/config.yaml")
    ap.add_argument("--raw", default="data/raw")
    ap.add_argument("--staged", default="data/staged")
    ap.add_argument("--quarantine", default="data/quarantine")
    ap.add_argument("--report", default="output/validation_report.json")
    ap.add_argument("--manifest", default="data/manifest.json")
    ap.add_argument("--months", nargs="*", help="subset, e.g. 2023-03 (default: all in manifest)")
    args = ap.parse_args(argv)

    cfg = load_config(Path(args.config))
    manifest = json.loads(Path(args.manifest).read_text())
    staged_dir, q_dir = Path(args.staged), Path(args.quarantine)
    staged_dir.mkdir(parents=True, exist_ok=True)
    q_dir.mkdir(parents=True, exist_ok=True)

    files = [f for f in manifest["files"]
             if not args.months or f["month"] in set(args.months)]
    if not files:
        print("no files selected", file=sys.stderr)
        return ABORT

    schema_results, file_results = [], []
    for f in files:
        month = f["month"]
        schema = check_schema(f["header"], month)
        schema_results.append(schema)
        if schema["status"] == "FAIL":
            print(f"SCHEMA FAIL {month}: missing {schema['missing_columns']}", file=sys.stderr)
            print("job abort — raw input does not match the contract", file=sys.stderr)
            return ABORT
        print(f"[validate] {month} …", flush=True)
        res = validate_file(Path(f["raw_path"]), month, cfg, staged_dir, q_dir)
        print(f"  rows={res['total_rows']:,} valid={res['valid_rows']:,} "
              f"quarantined={res['quarantined_rows']:,} ({res['seconds']}s)")
        file_results.append(res)

    manifest_rows = {f["month"]: f["row_count"] for f in files}
    report = build_report(file_results, schema_results, cfg, manifest_rows, files)

    if not all(c["ok"] for c in report["row_conservation"]):
        print("ROW CONSERVATION BROKEN — job abort", file=sys.stderr)
        return ABORT

    out = Path(args.report)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2))
    print_gate(report)
    print(f"\nreport → {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
