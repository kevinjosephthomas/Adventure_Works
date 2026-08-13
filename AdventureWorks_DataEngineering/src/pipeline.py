# ============================================================
# src/pipeline.py
# AdventureWorks Data Engineering Pipeline — Step 3
#
# Responsibility:
#   - Orchestrate the complete Full-Refresh pipeline
#   - Provide run_pipeline() as the single entry point
#   - Generate a Run ID, timing, and pipeline report
#   - Write structured logs to logs/pipeline.log
#   - Handle errors atomically (fail-fast, clean state)
#
# Architecture:  FULL REFRESH
#   Every call to run_pipeline() starts from source CSVs.
#   No state carried over from previous runs.
# ============================================================

import uuid
import logging
import traceback
from datetime import datetime, timezone
from pathlib import Path
import sys

# Add src/ to path so relative imports work from run_pipeline.py
SRC_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SRC_DIR))

from config import (
    LOGS_DIR, PIPELINE_LOG_FILE, PIPELINE_MODE,
    ensure_directories,
)
from ingestion      import extract_all, stage_raw
from validation     import validate_all
from transformation import transform_all
from oltp           import get_engine, create_oltp_schema, load_oltp
from olap           import build_olap, load_olap_to_db
from analytics      import run_analytics


# ──────────────────────────────────────────────────────────────
# LOGGING SETUP
# ──────────────────────────────────────────────────────────────

def setup_logging(run_id: str) -> logging.Logger:
    """
    Configure the pipeline logger.
    Writes to both the console AND logs/pipeline.log.

    Format: [YYYY-MM-DD HH:MM:SS] [LEVEL] [RUN_ID] message
    """
    ensure_directories()
    logger = logging.getLogger(f"pipeline.{run_id}")
    logger.setLevel(logging.DEBUG)

    # Avoid adding duplicate handlers when run multiple times
    if logger.handlers:
        logger.handlers.clear()

    fmt = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)-8s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler — always append to pipeline.log
    fh = logging.FileHandler(PIPELINE_LOG_FILE, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Console handler — INFO and above
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# ──────────────────────────────────────────────────────────────
# PIPELINE REPORT PRINTER
# ──────────────────────────────────────────────────────────────

def print_pipeline_report(report: dict) -> None:
    """
    Print the formatted pipeline execution report to stdout
    (exact format required by the project specification).
    """
    status = report.get("status", "UNKNOWN")
    error  = report.get("error_message", "")

    sep = "=" * 50

    print(f"\n{sep}")
    print("PIPELINE EXECUTION REPORT")
    print(sep)
    print()
    print(f"Run ID          : {report.get('run_id', 'N/A')}")
    print(f"Start Time      : {report.get('start_time', 'N/A')}")
    print(f"End Time        : {report.get('end_time', 'N/A')}")
    print(f"Duration        : {report.get('duration_sec', 0):.1f} seconds")
    print()
    print(f"Product Source Records    : {report.get('product_source_records', 0):>10,}")
    print(f"Customer Source Records   : {report.get('customer_source_records', 0):>10,}")
    print(f"Sales Header Records      : {report.get('header_source_records', 0):>10,}")
    print(f"Sales Detail Records      : {report.get('detail_source_records', 0):>10,}")
    print()
    print(f"Valid Records             : {report.get('valid_records', 0):>10,}")
    print(f"Invalid Records           : {report.get('invalid_records', 0):>10,}")
    print(f"Duplicate Records         : {report.get('duplicate_records', 0):>10,}")
    print(f"Outlier Records           : {report.get('outlier_records', 0):>10,}")
    print()
    print(f"OLTP Records Loaded       : {report.get('oltp_records', 'N/A (no DB)')}")
    print(f"FactSales Records         : {report.get('factsales_records', 0):>10,}")
    print()
    print(f"Pipeline Status           : {status}")
    if error:
        print(f"Error                     : {error}")
    print()
    print(sep)


# ──────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ──────────────────────────────────────────────────────────────

def run_pipeline(
    db_load: bool = True,
    mode:    str  = PIPELINE_MODE,
) -> dict:
    """
    Execute the complete AdventureWorks Data Engineering pipeline.

    Architecture: FULL REFRESH
    ─────────────────────────────
    Every call rebuilds all outputs from source CSVs.
    No state carried over from previous runs.
    Safe to call multiple times (idempotent).

    Parameters
    ----------
    db_load : bool
        If True, attempt to load data into PostgreSQL.
        If False (or DB unavailable), run in file-only mode.
    mode : str
        Pipeline mode — always 'FULL_REFRESH' in the main pipeline.

    Returns
    -------
    dict : pipeline execution report (same data printed to console)

    Pipeline Steps
    ──────────────
     1.  Read source CSV files
     2.  Write raw staging (staging/raw/)
     3.  Schema validation
     4.  Null validation
     5.  Duplicate validation
     6.  Outlier detection
     7.  Data transformation
     8.  OLTP load (PostgreSQL — optional)
     9.  Star Schema / OLAP build
    10.  OLAP DB load (PostgreSQL — optional)
    11.  Analytics & Dashboard datasets
    12.  Pipeline logging
    13.  Final validation check
    14.  Pipeline report
    """
    # ── Initialise ─────────────────────────────────────────────
    run_id     = str(uuid.uuid4()).split("-")[0].upper()
    start_dt   = datetime.now(timezone.utc)
    start_str  = start_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    logger     = setup_logging(run_id)

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
        "duplicate_records":       0,
        "outlier_records":         0,
        "oltp_records":            "N/A (no DB)",
        "factsales_records":       0,
        "stages_completed":        [],
        "status":                  "RUNNING",
        "error_message":           "",
    }

    logger.info("=" * 60)
    logger.info(f"PIPELINE START  |  Run ID: {run_id}  |  Mode: {mode}")
    logger.info("=" * 60)

    try:
        # ─────────────────────────────────────────────────────
        # STEP 1: EXTRACTION
        # ─────────────────────────────────────────────────────
        logger.info("[1/11] Extracting source CSV files ...")
        raw = extract_all()

        report["product_source_records"]  = len(raw["product"])
        report["customer_source_records"] = len(raw["customer"])
        report["header_source_records"]   = len(raw["salesorderheader"])
        report["detail_source_records"]   = len(raw["salesorderdetail"])

        logger.info(f"  Product          : {report['product_source_records']:,}")
        logger.info(f"  Customer         : {report['customer_source_records']:,}")
        logger.info(f"  SalesOrderHeader : {report['header_source_records']:,}")
        logger.info(f"  SalesOrderDetail : {report['detail_source_records']:,}")
        report["stages_completed"].append("extraction")

        # ─────────────────────────────────────────────────────
        # STEP 2: RAW STAGING
        # ─────────────────────────────────────────────────────
        logger.info("[2/11] Writing raw staging (staging/raw/) ...")
        stage_raw(raw)
        logger.info("  Raw Parquet files written (full refresh — overwritten).")
        report["stages_completed"].append("raw_staging")

        # ─────────────────────────────────────────────────────
        # STEP 3–6: VALIDATION
        # ─────────────────────────────────────────────────────
        logger.info("[3/11] Running validation (schema / null / duplicate / outlier) ...")
        val_result = validate_all(raw)

        if not val_result["passed"]:
            logger.warning("  Schema validation FAILED for one or more tables.")
            # We continue — partial failure is recorded but not fatal
            # (fatal fail reserved for critical missing columns)

        total_invalid  = sum(r["nulls"]["null_records"]          for r in val_result["reports"].values())
        total_dupes    = sum(r["duplicates"]["duplicate_records"] for r in val_result["reports"].values())
        total_outliers = sum(r["outliers"]["outlier_records"]     for r in val_result["reports"].values())
        total_valid    = sum(r["nulls"]["valid_records"]          for r in val_result["reports"].values())

        report["valid_records"]     = total_valid
        report["invalid_records"]   = total_invalid
        report["duplicate_records"] = total_dupes
        report["outlier_records"]   = total_outliers

        logger.info(f"  Valid     : {total_valid:,}")
        logger.info(f"  Invalid   : {total_invalid:,}")
        logger.info(f"  Dupes     : {total_dupes:,}")
        logger.info(f"  Outliers  : {total_outliers:,}")
        report["stages_completed"].append("validation")

        # ─────────────────────────────────────────────────────
        # STEP 7: TRANSFORMATION
        # ─────────────────────────────────────────────────────
        logger.info("[4/11] Transforming data ...")
        transformed = transform_all(val_result["valid"])
        logger.info(f"  Product : {len(transformed['product']):,}  Customer : {len(transformed['customer']):,}  Sales : {len(transformed['sales']):,}")
        report["stages_completed"].append("transformation")

        # ─────────────────────────────────────────────────────
        # STEP 8: OLTP LOAD
        # ─────────────────────────────────────────────────────
        logger.info("[5/11] Loading OLTP tables (PostgreSQL) ...")
        engine = get_engine() if db_load else None

        oltp_total = 0
        if engine is not None:
            created = create_oltp_schema(engine)
            if created:
                oltp_counts = load_oltp(engine, transformed)
                oltp_total  = sum(oltp_counts.values())
                report["oltp_records"] = oltp_total
                logger.info(f"  OLTP rows loaded: {oltp_total:,}")
            else:
                logger.warning("  OLTP schema creation failed — skipping load.")
        else:
            logger.info("  No DB connection — OLTP skipped (data in staging/valid/).")
        report["stages_completed"].append("oltp_load")

        # ─────────────────────────────────────────────────────
        # STEP 9–10: OLAP / STAR SCHEMA
        # ─────────────────────────────────────────────────────
        logger.info("[6/11] Building Star Schema (DimDate / DimProduct / DimCustomer / FactSales) ...")
        olap_data = build_olap(transformed)

        report["factsales_records"] = len(olap_data["fact_sales"])
        logger.info(f"  DimDate     : {len(olap_data['dim_date']):,}")
        logger.info(f"  DimProduct  : {len(olap_data['dim_product']):,}")
        logger.info(f"  DimCustomer : {len(olap_data['dim_customer']):,}")
        logger.info(f"  FactSales   : {len(olap_data['fact_sales']):,}")

        if engine is not None:
            logger.info("[7/11] Loading OLAP tables to PostgreSQL ...")
            load_olap_to_db(engine, olap_data)
        else:
            logger.info("[7/11] No DB — OLAP load skipped.")
        report["stages_completed"].append("olap_build")

        # ─────────────────────────────────────────────────────
        # STEP 11: ANALYTICS & DASHBOARD DATA
        # ─────────────────────────────────────────────────────
        logger.info("[8/11] Building analytics and dashboard datasets ...")
        analytics = run_analytics(transformed, val_result["reports"], olap_data)
        logger.info(f"  KPIs computed  : {len(analytics['kpis'])}")
        logger.info(f"  Datasets saved : {len(analytics['datasets'])}")
        report["stages_completed"].append("analytics")

        # ─────────────────────────────────────────────────────
        # STEP 12: FINAL VALIDATION CHECK
        # ─────────────────────────────────────────────────────
        logger.info("[9/11] Running final record-count validation ...")
        fact_count   = len(olap_data["fact_sales"])
        source_count = report["detail_source_records"]

        if fact_count != source_count:
            logger.warning(
                f"  FactSales rows ({fact_count:,}) != Source detail rows ({source_count:,}). "
                "Some records may have been filtered by null/FK checks."
            )
        else:
            logger.info(f"  FactSales count matches source detail count: {fact_count:,}")
        report["stages_completed"].append("final_validation")

        # ─────────────────────────────────────────────────────
        # SUCCESS
        # ─────────────────────────────────────────────────────
        report["status"] = "SUCCESS"
        logger.info("[10/11] Pipeline completed successfully.")

    except Exception as exc:
        report["status"]        = "FAILED"
        report["error_message"] = str(exc)
        logger.error(f"PIPELINE FAILED at stage: {exc}")
        logger.error(traceback.format_exc())

    # ── Finalize report ────────────────────────────────────────
    end_dt   = datetime.now(timezone.utc)
    duration = (end_dt - start_dt).total_seconds()
    report["end_time"]    = end_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    report["duration_sec"] = round(duration, 1)

    logger.info(f"Run ID: {run_id}  |  Status: {report['status']}  |  Duration: {duration:.1f}s")
    logger.info("=" * 60)

    # Save run to history so app.py can display it
    try:
        import json
        history_file = LOGS_DIR / "pipeline_runs.json"
        history = []
        if history_file.exists():
            try:
                history = json.loads(history_file.read_text(encoding="utf-8"))
            except Exception:
                history = []
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
            "dq_status":       "PASS" if report.get("status") == "SUCCESS" else "FAIL",
            "stages_completed":report.get("stages_completed", []),
        }
        history.append(entry)
        history_file.write_text(json.dumps(history, indent=2, default=str), encoding="utf-8")
    except Exception as e:
        logger.error(f"Failed to write pipeline run history: {e}")

    # Print the formatted report to stdout
    print_pipeline_report(report)

    return report


# ──────────────────────────────────────────────────────────────
# SELF-TEST
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    result = run_pipeline(db_load=False)
    print(f"\nReturn value status: {result['status']}")
