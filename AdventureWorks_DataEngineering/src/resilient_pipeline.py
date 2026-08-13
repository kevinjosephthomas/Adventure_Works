# ============================================================
# src/resilient_pipeline.py
# AdventureWorks Data Engineering Pipeline — Step 4
#
# Responsibility:
#   - Pre-flight health checks (source files, disk, dirs)
#   - Retry logic with exponential backoff for transient failures
#   - Atomic staging writes (temp → rename → final)
#   - Data Quality rules engine integration
#   - Pipeline run history (logs/pipeline_runs.json)
#   - Alert thresholds with actionable messages
#   - Stage-level state tracking (JSON state file)
#   - Graceful degradation on WARNING-level DQ failures
#   - Hard halt on ERROR-level DQ failures
# ============================================================

import json
import time
import shutil
import uuid
import logging
import traceback
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Callable, Any, Optional
import sys

SRC_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SRC_DIR))

from config import (
    PROJECT_ROOT, SOURCE_FILES, DATA_PATH,
    STAGING_RAW, STAGING_VALID, STAGING_INVALID,
    STAGING_DUPLICATES, STAGING_OUTLIERS,
    LOGS_DIR, PIPELINE_LOG_FILE, DASHBOARD_DIR,
    PIPELINE_MODE, ensure_directories,
)
from ingestion      import extract_all, stage_raw
from validation     import validate_all
from transformation import transform_all
from oltp           import get_engine, create_oltp_schema, load_oltp
from olap           import build_olap, load_olap_to_db
from analytics      import run_analytics
from data_quality   import (
    run_data_quality_checks, print_dq_report,
    save_dq_results, DEFAULT_RULES, DQRule,
)


# ──────────────────────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────────────────────

PIPELINE_RUNS_FILE  = LOGS_DIR / "pipeline_runs.json"
PIPELINE_STATE_FILE = LOGS_DIR / "pipeline_state.json"

# Alert thresholds
ALERT_OUTLIER_RATIO   = 0.20   # warn if > 20% of any table is outlier
ALERT_INVALID_RATIO   = 0.005  # warn if > 0.5% of any table is invalid
ALERT_MIN_SALES_VALUE = 1_000_000.0   # warn if total sales < $1M

# Retry settings
DEFAULT_MAX_RETRIES  = 3
DEFAULT_BACKOFF_BASE = 2.0   # seconds (doubles each attempt)


# ──────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────

def setup_logger(run_id: str) -> logging.Logger:
    """Configure logger with file + console handlers."""
    ensure_directories()
    logger = logging.getLogger(f"resilient.{run_id}")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        logger.handlers.clear()

    fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)-8s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = logging.FileHandler(PIPELINE_LOG_FILE, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ──────────────────────────────────────────────────────────────
# HEALTH CHECKS
# ──────────────────────────────────────────────────────────────

def run_health_checks() -> dict:
    """
    Pre-flight checks before any pipeline work starts.

    Checks
    ------
    1. Source files exist and are readable
    2. Source files are non-empty (> 0 bytes)
    3. Required staging directories are writable
    4. Dashboard directory is writable
    5. Logs directory is writable

    Returns
    -------
    dict with 'passed' (bool), 'checks' (list of dicts)
    """
    ensure_directories()
    checks = []
    all_passed = True

    # ── 1. Source file existence & size ───────────────────────
    for table_name, file_path in SOURCE_FILES.items():
        exists   = file_path.exists()
        readable = False
        size_kb  = 0

        if exists:
            try:
                size_kb  = file_path.stat().st_size / 1024
                readable = size_kb > 0
            except Exception:
                pass

        passed = exists and readable
        checks.append({
            "check":   f"source_file_{table_name}",
            "passed":  passed,
            "detail":  f"{file_path.name} — {'found' if exists else 'MISSING'}, {size_kb:.1f} KB",
        })
        if not passed:
            all_passed = False

    # ── 2. Write test on staging directories ──────────────────
    for name, path in [
        ("staging_raw",        STAGING_RAW),
        ("staging_valid",      STAGING_VALID),
        ("staging_invalid",    STAGING_INVALID),
        ("staging_duplicates", STAGING_DUPLICATES),
        ("staging_outliers",   STAGING_OUTLIERS),
        ("dashboard",          DASHBOARD_DIR),
        ("logs",               LOGS_DIR),
    ]:
        try:
            test_file = path / ".write_test"
            test_file.write_text("ok", encoding="utf-8")
            test_file.unlink()
            checks.append({"check": f"writable_{name}", "passed": True,
                           "detail": f"{path} — writable"})
        except Exception as exc:
            checks.append({"check": f"writable_{name}", "passed": False,
                           "detail": f"{path} — NOT writable: {exc}"})
            all_passed = False

    # ── 3. Python import check ─────────────────────────────────
    try:
        import pyarrow  # noqa: F401
        checks.append({"check": "import_pyarrow", "passed": True, "detail": "pyarrow available"})
    except ImportError:
        checks.append({"check": "import_pyarrow", "passed": False,
                       "detail": "pyarrow not installed — Parquet IO will fail"})
        all_passed = False

    return {"passed": all_passed, "checks": checks}


def print_health_report(result: dict) -> None:
    """Print the pre-flight health check report."""
    print("\n" + "=" * 60)
    print("  PRE-FLIGHT HEALTH CHECKS")
    print("=" * 60)
    for c in result["checks"]:
        tag = "[OK]  " if c["passed"] else "[FAIL]"
        print(f"  {tag}  {c['detail']}")
    status = "PASSED" if result["passed"] else "FAILED — cannot proceed"
    print(f"\n  Health Check Status: {status}")
    print("=" * 60)


# ──────────────────────────────────────────────────────────────
# RETRY LOGIC
# ──────────────────────────────────────────────────────────────

def with_retry(
    func:         Callable,
    args:         tuple = (),
    kwargs:       dict  = None,
    max_retries:  int   = DEFAULT_MAX_RETRIES,
    backoff_base: float = DEFAULT_BACKOFF_BASE,
    logger:       Optional[logging.Logger] = None,
    stage_name:   str   = "stage",
) -> Any:
    """
    Execute a callable with exponential backoff retry on failure.

    Wait schedule: 2s, 4s, 8s (for backoff_base=2, max_retries=3)

    Parameters
    ----------
    func         : callable to execute
    args         : positional arguments
    kwargs       : keyword arguments
    max_retries  : max number of attempts (including first try)
    backoff_base : seconds to wait before retry 1; doubles each attempt
    logger       : optional logger for retry messages
    stage_name   : human-readable name for log messages

    Returns
    -------
    Return value of func on success.

    Raises
    ------
    Last exception if all attempts fail.
    """
    kwargs = kwargs or {}
    last_exc = None

    for attempt in range(1, max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                wait = backoff_base ** (attempt - 1)
                msg  = f"[{stage_name}] Attempt {attempt}/{max_retries} failed: {exc}. Retrying in {wait:.0f}s ..."
                if logger:
                    logger.warning(msg)
                else:
                    print(f"  [RETRY] {msg}")
                time.sleep(wait)
            else:
                msg = f"[{stage_name}] All {max_retries} attempts failed."
                if logger:
                    logger.error(msg)
                else:
                    print(f"  [FAIL]  {msg}")

    raise last_exc


# ──────────────────────────────────────────────────────────────
# ATOMIC STAGING WRITE
# ──────────────────────────────────────────────────────────────

def atomic_stage(raw: dict, logger: Optional[logging.Logger] = None) -> bool:
    """
    Write raw Parquet files atomically.

    Strategy:
      1. Write to a temporary directory (staging/raw/.tmp_<uuid>/)
      2. If all files written successfully → rename temp dir to final
      3. If any write fails → delete temp dir (no partial state left)

    This prevents a partially-written staging folder from being read
    by a concurrent or subsequent pipeline run.

    Returns True on success, False on failure.
    """
    import pandas as pd

    tmp_dir  = STAGING_RAW / f".tmp_{uuid.uuid4().hex[:8]}"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    try:
        for table_name, df in raw.items():
            out_path = tmp_dir / f"raw_{table_name}.parquet"
            df.to_parquet(out_path, index=False, engine="pyarrow")

        # ── Atomic rename: move each file to final location ───
        for file in tmp_dir.iterdir():
            final = STAGING_RAW / file.name
            if final.exists():
                final.unlink()   # remove old version
            shutil.move(str(file), str(final))

        if logger:
            logger.info(f"  Atomic staging complete: {len(raw)} files written.")
        return True

    except Exception as exc:
        # Clean up temp directory — no partial state
        shutil.rmtree(str(tmp_dir), ignore_errors=True)
        if logger:
            logger.error(f"  Atomic staging FAILED: {exc}")
        return False
    finally:
        # Always clean up temp dir
        if tmp_dir.exists():
            shutil.rmtree(str(tmp_dir), ignore_errors=True)


# ──────────────────────────────────────────────────────────────
# PIPELINE STATE TRACKING
# ──────────────────────────────────────────────────────────────

def save_pipeline_state(state: dict) -> None:
    """Persist the current pipeline state to logs/pipeline_state.json."""
    ensure_directories()
    PIPELINE_STATE_FILE.write_text(
        json.dumps(state, indent=2, default=str),
        encoding="utf-8",
    )


def load_pipeline_state() -> dict:
    """Load the most recent pipeline state (or empty dict if none)."""
    if PIPELINE_STATE_FILE.exists():
        try:
            return json.loads(PIPELINE_STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


# ──────────────────────────────────────────────────────────────
# RUN HISTORY
# ──────────────────────────────────────────────────────────────

def save_run_history(report: dict) -> None:
    """
    Append the pipeline report to logs/pipeline_runs.json.
    The file grows over time — keeps a full audit trail of all runs.
    """
    ensure_directories()
    history = []
    if PIPELINE_RUNS_FILE.exists():
        try:
            history = json.loads(PIPELINE_RUNS_FILE.read_text(encoding="utf-8"))
        except Exception:
            history = []

    # Compact entry for the run history file
    entry = {
        "run_id":          report.get("run_id"),
        "start_time":      report.get("start_time"),
        "end_time":        report.get("end_time"),
        "duration_sec":    report.get("duration_sec"),
        "mode":            report.get("mode", "FULL_REFRESH"),
        "status":          report.get("status"),
        "error_message":   report.get("error_message", ""),
        "product_records": report.get("product_source_records", 0),
        "customer_records":report.get("customer_source_records", 0),
        "header_records":  report.get("header_source_records", 0),
        "detail_records":  report.get("detail_source_records", 0),
        "factsales_rows":  report.get("factsales_records", 0),
        "dq_passed":       report.get("dq_passed", 0),
        "dq_warnings":     report.get("dq_warnings", 0),
        "dq_errors":       report.get("dq_errors", 0),
        "dq_status":       report.get("dq_status", "UNKNOWN"),
        "stages_completed":report.get("stages_completed", []),
    }
    history.append(entry)

    PIPELINE_RUNS_FILE.write_text(
        json.dumps(history, indent=2, default=str),
        encoding="utf-8",
    )


def load_run_history() -> list:
    """Load all historical pipeline run records."""
    if PIPELINE_RUNS_FILE.exists():
        try:
            return json.loads(PIPELINE_RUNS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []


# ──────────────────────────────────────────────────────────────
# THRESHOLD ALERTS
# ──────────────────────────────────────────────────────────────

def check_alert_thresholds(
    val_result:  dict,
    transformed: dict,
    logger:      Optional[logging.Logger] = None,
) -> list[str]:
    """
    Check pipeline-level alert thresholds after validation.

    Returns a list of alert messages (empty if all thresholds met).
    These are additional checks beyond the DQ rules.
    """
    alerts = []

    for table_name, rep in val_result["reports"].items():
        total  = rep["nulls"]["valid_records"] + rep["nulls"]["null_records"]
        if total == 0:
            continue

        # Alert: invalid ratio
        inv_ratio = rep["nulls"]["null_records"] / total
        if inv_ratio > ALERT_INVALID_RATIO:
            msg = (f"ALERT [{table_name}] Invalid record ratio {inv_ratio*100:.2f}% "
                   f"exceeds threshold {ALERT_INVALID_RATIO*100:.1f}%")
            alerts.append(msg)
            if logger:
                logger.warning(msg)

        # Alert: outlier ratio
        out_count = rep["outliers"]["outlier_records"]
        out_ratio = out_count / total
        if out_ratio > ALERT_OUTLIER_RATIO:
            msg = (f"ALERT [{table_name}] Outlier ratio {out_ratio*100:.2f}% "
                   f"({out_count:,} records) exceeds threshold {ALERT_OUTLIER_RATIO*100:.1f}%")
            alerts.append(msg)
            if logger:
                logger.warning(msg)

    # Alert: total sales value
    total_sales = transformed["sales"]["LineTotal"].sum()
    if total_sales < ALERT_MIN_SALES_VALUE:
        msg = (f"ALERT Total sales ${total_sales:,.2f} is below the alert "
               f"threshold of ${ALERT_MIN_SALES_VALUE:,.2f}")
        alerts.append(msg)
        if logger:
            logger.warning(msg)

    return alerts


# ──────────────────────────────────────────────────────────────
# PIPELINE REPORT PRINTER
# ──────────────────────────────────────────────────────────────

def print_resilient_report(report: dict) -> None:
    """Print the resilient pipeline execution report."""
    sep = "=" * 55
    print(f"\n{sep}")
    print("  RESILIENT PIPELINE EXECUTION REPORT")
    print(sep)
    print()
    print(f"  Run ID          : {report.get('run_id')}")
    print(f"  Start           : {report.get('start_time')}")
    print(f"  End             : {report.get('end_time')}")
    print(f"  Duration        : {report.get('duration_sec', 0):.1f}s")
    print()
    print(f"  Product         : {report.get('product_source_records', 0):>10,}")
    print(f"  Customer        : {report.get('customer_source_records', 0):>10,}")
    print(f"  SalesHeader     : {report.get('header_source_records', 0):>10,}")
    print(f"  SalesDetail     : {report.get('detail_source_records', 0):>10,}")
    print()
    print(f"  Valid Records   : {report.get('valid_records', 0):>10,}")
    print(f"  Invalid Records : {report.get('invalid_records', 0):>10,}")
    print(f"  Outlier Records : {report.get('outlier_records', 0):>10,}")
    print()
    print(f"  FactSales Rows  : {report.get('factsales_records', 0):>10,}")
    print(f"  OLTP Loaded     : {report.get('oltp_records', 'N/A')}")
    print()
    print(f"  DQ Rules Passed : {report.get('dq_passed', 0):>5} / {report.get('dq_total', 0)}")
    print(f"  DQ Warnings     : {report.get('dq_warnings', 0):>5}")
    print(f"  DQ Errors       : {report.get('dq_errors', 0):>5}")
    print(f"  DQ Status       : {report.get('dq_status', 'UNKNOWN')}")
    print()
    alerts = report.get("alerts", [])
    if alerts:
        print(f"  ALERTS ({len(alerts)}):")
        for a in alerts:
            print(f"    {a}")
        print()
    print(f"  Pipeline Status : {report.get('status', 'UNKNOWN')}")
    if report.get("error_message"):
        print(f"  Error           : {report.get('error_message')}")
    print()
    print(f"  Stages Completed: {', '.join(report.get('stages_completed', []))}")
    print()
    print(sep)


# ──────────────────────────────────────────────────────────────
# MAIN: RESILIENT PIPELINE
# ──────────────────────────────────────────────────────────────

def run_resilient_pipeline(
    db_load:      bool         = True,
    mode:         str          = PIPELINE_MODE,
    dq_rules:     list[DQRule] = None,
    skip_health:  bool         = False,
    halt_on_error: bool        = True,
) -> dict:
    """
    Run the complete pipeline with all resilience features.

    Features vs run_pipeline()
    --------------------------
    + Pre-flight health checks  (skip_health=False by default)
    + Configurable DQ rules engine  (halt_on_error if ERROR-level fails)
    + Retry with exponential backoff  (extract + DB operations)
    + Atomic staging writes  (temp → rename → final)
    + Pipeline state tracking  (logs/pipeline_state.json)
    + Run history persistence  (logs/pipeline_runs.json)
    + Threshold alerts  (outlier%, invalid%, total sales)

    Parameters
    ----------
    db_load       : attempt PostgreSQL load if True
    mode          : pipeline mode (always FULL_REFRESH)
    dq_rules      : custom DQ rule list (defaults to DEFAULT_RULES)
    skip_health   : skip pre-flight checks (for testing)
    halt_on_error : halt pipeline if DQ errors are found

    Returns
    -------
    dict : complete pipeline execution report
    """
    # ── Initialise ─────────────────────────────────────────────
    run_id    = str(uuid.uuid4()).split("-")[0].upper()
    start_dt  = datetime.now(timezone.utc)
    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    logger    = setup_logger(run_id)

    report = {
        "run_id":                  run_id,
        "start_time":              start_str,
        "end_time":                None,
        "duration_sec":            0.0,
        "mode":                    mode,
        "product_source_records":  0,
        "customer_source_records": 0,
        "header_source_records":   0,
        "detail_source_records":   0,
        "valid_records":           0,
        "invalid_records":         0,
        "outlier_records":         0,
        "oltp_records":            "N/A (no DB)",
        "factsales_records":       0,
        "dq_passed":               0,
        "dq_warnings":             0,
        "dq_errors":               0,
        "dq_total":                0,
        "dq_status":               "NOT_RUN",
        "alerts":                  [],
        "stages_completed":        [],
        "status":                  "RUNNING",
        "error_message":           "",
    }

    # ── Save initial state ─────────────────────────────────────
    save_pipeline_state({"run_id": run_id, "status": "RUNNING", "stage": "init"})

    logger.info("=" * 60)
    logger.info(f"RESILIENT PIPELINE START  |  Run ID: {run_id}")
    logger.info("=" * 60)

    try:
        # ─────────────────────────────────────────────────────
        # STAGE 0: PRE-FLIGHT HEALTH CHECKS
        # ─────────────────────────────────────────────────────
        if not skip_health:
            logger.info("[0] Running pre-flight health checks ...")
            save_pipeline_state({"run_id": run_id, "status": "RUNNING", "stage": "health_check"})
            health = run_health_checks()

            if not health["passed"]:
                failed = [c["detail"] for c in health["checks"] if not c["passed"]]
                raise RuntimeError(f"Health checks failed: {failed}")

            logger.info(f"  Health checks: PASSED ({len(health['checks'])} checks)")
            report["stages_completed"].append("health_check")

        # ─────────────────────────────────────────────────────
        # STAGE 1: EXTRACTION  (with retry)
        # ─────────────────────────────────────────────────────
        logger.info("[1] Extracting source data (with retry) ...")
        save_pipeline_state({"run_id": run_id, "status": "RUNNING", "stage": "extraction"})

        raw = with_retry(
            extract_all,
            max_retries=DEFAULT_MAX_RETRIES,
            backoff_base=DEFAULT_BACKOFF_BASE,
            logger=logger,
            stage_name="extract_all",
        )

        report["product_source_records"]  = len(raw["product"])
        report["customer_source_records"] = len(raw["customer"])
        report["header_source_records"]   = len(raw["salesorderheader"])
        report["detail_source_records"]   = len(raw["salesorderdetail"])

        logger.info(f"  Extracted {sum(len(v) for v in raw.values()):,} total records")
        report["stages_completed"].append("extraction")

        # ─────────────────────────────────────────────────────
        # STAGE 2: ATOMIC STAGING
        # ─────────────────────────────────────────────────────
        logger.info("[2] Writing raw staging (atomic) ...")
        save_pipeline_state({"run_id": run_id, "status": "RUNNING", "stage": "raw_staging"})

        ok = atomic_stage(raw, logger)
        if not ok:
            raise RuntimeError("Atomic staging write failed — aborting to preserve data integrity.")
        report["stages_completed"].append("raw_staging")

        # ─────────────────────────────────────────────────────
        # STAGE 3: VALIDATION
        # ─────────────────────────────────────────────────────
        logger.info("[3] Validating data ...")
        save_pipeline_state({"run_id": run_id, "status": "RUNNING", "stage": "validation"})

        val_result = validate_all(raw)

        total_invalid  = sum(r["nulls"]["null_records"]          for r in val_result["reports"].values())
        total_dupes    = sum(r["duplicates"]["duplicate_records"] for r in val_result["reports"].values())
        total_outliers = sum(r["outliers"]["outlier_records"]     for r in val_result["reports"].values())
        total_valid    = sum(r["nulls"]["valid_records"]          for r in val_result["reports"].values())

        report["valid_records"]    = total_valid
        report["invalid_records"]  = total_invalid
        report["outlier_records"]  = total_outliers
        logger.info(f"  Valid={total_valid:,}  Invalid={total_invalid:,}  Outliers={total_outliers:,}")
        report["stages_completed"].append("validation")

        # ─────────────────────────────────────────────────────
        # STAGE 4: DATA QUALITY RULES ENGINE
        # ─────────────────────────────────────────────────────
        logger.info("[4] Running DQ rules engine ...")
        save_pipeline_state({"run_id": run_id, "status": "RUNNING", "stage": "data_quality"})

        dq_results, dq_summary = run_data_quality_checks(raw, val_result["reports"], dq_rules)
        save_dq_results(dq_results, run_id, LOGS_DIR)

        report["dq_passed"]   = dq_summary["passed"]
        report["dq_warnings"] = dq_summary["warnings"]
        report["dq_errors"]   = dq_summary["errors"]
        report["dq_total"]    = dq_summary["total_rules"]
        report["dq_status"]   = dq_summary["overall_status"]

        logger.info(
            f"  DQ: {dq_summary['passed']}/{dq_summary['total_rules']} passed  "
            f"Warnings={dq_summary['warnings']}  Errors={dq_summary['errors']}"
        )

        if dq_summary["errors"] > 0:
            logger.error(f"  {dq_summary['errors']} ERROR-level DQ rule(s) failed:")
            for r in dq_results:
                if not r.passed and r.severity == "ERROR":
                    logger.error(f"    [{r.rule_id}] {r.message}")

            if halt_on_error:
                raise RuntimeError(
                    f"Pipeline halted: {dq_summary['errors']} ERROR-level DQ rule(s) failed. "
                    "Fix the source data or adjust DQ thresholds."
                )

        if dq_summary["warnings"] > 0:
            for r in dq_results:
                if not r.passed and r.severity == "WARNING":
                    logger.warning(f"  [DQ WARNING] {r.rule_id}: {r.message}")

        report["stages_completed"].append("data_quality")

        # ─────────────────────────────────────────────────────
        # STAGE 5: TRANSFORMATION
        # ─────────────────────────────────────────────────────
        logger.info("[5] Transforming data ...")
        save_pipeline_state({"run_id": run_id, "status": "RUNNING", "stage": "transformation"})

        transformed = transform_all(val_result["valid"])
        logger.info(f"  Product={len(transformed['product']):,}  "
                    f"Customer={len(transformed['customer']):,}  "
                    f"Sales={len(transformed['sales']):,}")
        report["stages_completed"].append("transformation")

        # ─────────────────────────────────────────────────────
        # STAGE 6: THRESHOLD ALERTS
        # ─────────────────────────────────────────────────────
        logger.info("[6] Checking alert thresholds ...")
        alerts = check_alert_thresholds(val_result, transformed, logger)
        report["alerts"] = alerts
        if alerts:
            logger.warning(f"  {len(alerts)} alert(s) triggered")
        else:
            logger.info("  All thresholds within acceptable range.")
        report["stages_completed"].append("alerts")

        # ─────────────────────────────────────────────────────
        # STAGE 7: OLTP LOAD  (with retry)
        # ─────────────────────────────────────────────────────
        logger.info("[7] Loading OLTP tables ...")
        save_pipeline_state({"run_id": run_id, "status": "RUNNING", "stage": "oltp_load"})

        oltp_total = 0
        if db_load:
            try:
                engine = with_retry(
                    get_engine,
                    max_retries=2,
                    backoff_base=3.0,
                    logger=logger,
                    stage_name="db_connect",
                )
                if engine:
                    created = create_oltp_schema(engine)
                    if created:
                        oltp_counts = load_oltp(engine, transformed)
                        oltp_total  = sum(oltp_counts.values())
                        report["oltp_records"] = oltp_total
                        logger.info(f"  OLTP loaded: {oltp_total:,} rows")
            except Exception as e:
                logger.warning(f"  OLTP load failed (non-fatal): {e}")
        else:
            logger.info("  OLTP skipped (db_load=False).")
        report["stages_completed"].append("oltp_load")

        # ─────────────────────────────────────────────────────
        # STAGE 8: STAR SCHEMA / OLAP
        # ─────────────────────────────────────────────────────
        logger.info("[8] Building Star Schema ...")
        save_pipeline_state({"run_id": run_id, "status": "RUNNING", "stage": "olap_build"})

        olap_data = build_olap(transformed)
        report["factsales_records"] = len(olap_data["fact_sales"])
        logger.info(f"  FactSales: {len(olap_data['fact_sales']):,} rows")
        report["stages_completed"].append("olap_build")

        # ─────────────────────────────────────────────────────
        # STAGE 9: ANALYTICS & DASHBOARD DATA
        # ─────────────────────────────────────────────────────
        logger.info("[9] Building analytics datasets ...")
        analytics = run_analytics(transformed, val_result["reports"], olap_data)
        logger.info(f"  KPIs={len(analytics['kpis'])}  Datasets={len(analytics['datasets'])}")
        report["stages_completed"].append("analytics")

        # ─────────────────────────────────────────────────────
        # STAGE 10: FINAL COUNT VALIDATION
        # ─────────────────────────────────────────────────────
        logger.info("[10] Final record-count validation ...")
        fact_count   = len(olap_data["fact_sales"])
        source_count = report["detail_source_records"]
        if fact_count != source_count:
            logger.warning(f"  FactSales ({fact_count:,}) != Source detail ({source_count:,})")
        else:
            logger.info(f"  FactSales matches source: {fact_count:,}")
        report["stages_completed"].append("final_validation")

        # ─────────────────────────────────────────────────────
        # SUCCESS
        # ─────────────────────────────────────────────────────
        report["status"] = "SUCCESS"
        logger.info("Pipeline completed: SUCCESS")

    except Exception as exc:
        report["status"]        = "FAILED"
        report["error_message"] = str(exc)
        
        # Capture the failed stage
        try:
            last_state = load_pipeline_state()
            if last_state.get("run_id") == run_id:
                report["failed_stage"] = last_state.get("stage", "unknown")
        except Exception:
            pass

        logger.error(f"PIPELINE FAILED: {exc}")
        logger.error(traceback.format_exc())

    # ── Finalise ───────────────────────────────────────────────
    end_dt             = datetime.now(timezone.utc)
    report["end_time"] = end_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    report["duration_sec"] = round((end_dt - start_dt).total_seconds(), 1)

    logger.info(f"Run ID: {run_id}  Status: {report['status']}  Duration: {report['duration_sec']}s")
    logger.info("=" * 60)

    # Save final state and run history
    final_stage = "done" if report["status"] == "SUCCESS" else report.get("failed_stage", "done")
    save_pipeline_state({"run_id": run_id, "status": report["status"], "stage": final_stage})
    save_run_history(report)

    # Print formatted report
    print_resilient_report(report)

    return report


# ──────────────────────────────────────────────────────────────
# SELF-TEST
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    result = run_resilient_pipeline(db_load=False)
    print(f"\nStatus: {result['status']}")
    history = load_run_history()
    print(f"Total runs in history: {len(history)}")
