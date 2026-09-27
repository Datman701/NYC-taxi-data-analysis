"""Stage runner — retrieve → ingest → validate → transform → metrics, with logging & reruns.

    PYTHONPATH=src python -m nyc_pipeline.run --source ../archive

Exit codes
----------
0  pipeline completed (gate status is in the report/run manifest; see --require-publish)
1  a step raised (details in logs/ + output/run_manifest.json)
3  validation contract abort (schema mismatch / row conservation broken)
4  completed but gate == HOLD and --require-publish was set

Rerun behaviour: raw is read-only; every output is rewritten atomically; running twice
with the same inputs yields identical metric files (see README.md §7).
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

from . import api_fetch, ingest, metrics, transform, validate

log = logging.getLogger("nyc_pipeline")


def setup_logging(run_id: str, log_dir: Path) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"run_{run_id}.log"
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    for h in (logging.StreamHandler(sys.stdout),
              logging.FileHandler(log_file, encoding="utf-8")):
        h.setFormatter(fmt)
        log.addHandler(h)
    return log_file


def git_commit() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=Path(__file__).resolve().parents[2], stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:  # noqa: BLE001
        return None


def step(name: str, fn, steps_out: list[dict], **kwargs) -> object:
    log.info("─" * 30)
    log.info("STEP %s start", name)
    t0 = time.time()
    try:
        result = fn(**kwargs)
    except SystemExit as e:                    # argparse mains
        result = e.code
    except Exception as e:  # noqa: BLE001
        log.error("STEP %s FAILED: %s", name, e)
        log.error(traceback.format_exc())
        steps_out.append({"name": name, "status": "FAILED", "seconds": round(time.time() - t0, 1),
                          "error": str(e)})
        raise
    secs = round(time.time() - t0, 1)
    rc = result if isinstance(result, int) else 0
    status = "OK" if rc == 0 else f"ABORT(rc={rc})"
    steps_out.append({"name": name, "status": status, "seconds": secs})
    log.info("STEP %s %s (%ss)", name, status, secs)
    if rc != 0:
        raise RuntimeError(f"step {name} aborted with rc={rc}")
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run the full monthly metrics pipeline")
    ap.add_argument("--force-fetch", action="store_true",
                    help="re-retrieve months even if their parquet already exists")
    ap.add_argument("--config", default="src/nyc_pipeline/config.yaml")
    ap.add_argument("--months", nargs="*", help="subset of months")
    ap.add_argument("--baseline", default="2023-01")
    ap.add_argument("--refresh-api", action="store_true", help="force API re-retrieval")
    ap.add_argument("--require-publish", action="store_true",
                    help="exit 4 if the validation gate is HOLD")
    args = ap.parse_args(argv)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = setup_logging(run_id, Path("logs"))
    started = datetime.now(timezone.utc).isoformat()
    steps: list[dict] = []
    log.info("run %s | python %s | git %s", run_id, sys.version.split()[0], git_commit())

    argv_common = []
    if args.months:
        argv_common += ["--months", *args.months]

    try:
        # 1. RETRIEVE — Socrata API, per-month, count-reconciled (idempotent/resumable)
        raw_dir = Path("data/raw")
        have = sorted(raw_dir.glob("yellow_tripdata_*.parquet"))
        if have and not args.refresh_api and not args.force_fetch:
            log.info("STEP api_fetch SKIPPED (%d month parquet present; use --refresh-api)",
                     len(have))
            steps.append({"name": "api_fetch", "status": f"SKIPPED({len(have)} months present)"})
        else:
            step("retrieve", api_fetch.main, steps, argv=["--out", "data/reference"])

        # 2. INGEST — fingerprint what we retrieved (never modifies it)
        step("ingest", ingest.main, steps,
             argv=["--raw", str(raw_dir), "--manifest", "data/manifest.json"])

        step("validate", validate.main, steps,
             argv=["--config", args.config, *argv_common])
        step("transform", transform.main, steps, argv=argv_common)
        step("metrics", metrics.main, steps,
             argv=["--baseline", args.baseline])

    except Exception as e:  # noqa: BLE001
        log.error("PIPELINE FAILED: %s", e)
        _write_run_manifest(run_id, started, steps, "FAILED", None, log_file)
        return 1 if not str(e).startswith("step validate aborted") else 3

    report = json.loads(Path("output/validation_report.json").read_text())
    gate = report["gate"]["status"]
    log.info("GATE: %s | coverage=%.4f | valid=%d / %d",
             gate, report["totals"]["coverage"],
             report["totals"]["valid_rows"], report["totals"]["raw_rows"])
    manifest = _write_run_manifest(run_id, started, steps, "COMPLETED", gate, log_file)
    log.info("run manifest → %s", manifest)

    if args.require_publish and gate != "PUBLISH":
        log.error("--require-publish set and gate is %s", gate)
        return 4
    return 0


def _write_run_manifest(run_id: str, started: str, steps: list[dict], status: str,
                        gate: str | None, log_file: Path) -> Path:
    out = Path("output/run_manifest.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    counts = {}
    try:
        r = json.loads(Path("output/validation_report.json").read_text())
        counts = {"raw_rows": r["totals"]["raw_rows"],
                  "valid_rows": r["totals"]["valid_rows"],
                  "quarantined_rows": r["totals"]["quarantined_rows"]}
    except Exception:  # noqa: BLE001
        pass
    payload = {
        "run_id": run_id, "started_at": started,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "status": status, "gate": gate,
        "git_commit": git_commit(), "log_file": str(log_file),
        "steps": steps, "row_counts": counts,
        "idempotency": "raw immutable; outputs rewritten atomically; rerun → same metrics",
    }
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2))
    tmp.replace(out)                              # atomic write
    return out


if __name__ == "__main__":
    raise SystemExit(main())
