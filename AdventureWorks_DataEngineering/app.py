# ============================================================
# app.py
# AdventureWorks Data Engineering — Flask Dashboard (Step 5)
#
# Architecture:
#   Flask = UI + CSV management + Pipeline trigger + Visualization
#   Flask does NOT touch OLTP/OLAP directly.
#   All data mutations go through: CSV → run_pipeline() → outputs.
#
# Data flow:
#   User action → update CSV → run_pipeline() → read dashboard/ files → render
# ============================================================

import os
import sys
import json
import csv
import uuid
import threading
import traceback
from pathlib import Path
from datetime import datetime, timezone
from functools import wraps

from flask import (
    Flask, render_template, request, redirect,
    url_for, flash, jsonify, abort,
)
import pandas as pd

# ── Project path setup ──────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
SRC_DIR  = BASE_DIR / "src"
sys.path.insert(0, str(SRC_DIR))

from src.config import (
    PROJECT_ROOT, SOURCE_FILES, DATA_PATH,
    DASHBOARD_DIR, LOGS_DIR, PIPELINE_LOG_FILE,
    ensure_directories,
)

# ── Import pipeline (reuse existing — do NOT duplicate logic) ─
from src.resilient_pipeline import run_resilient_pipeline as run_pipeline

# ── Flask app ────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "aw-de-pipeline-secret-2025")

ensure_directories()

# ── Jinja2 custom filters ────────────────────────────────────
@app.template_filter('format_number')
def format_number(value):
    """Format integer with commas: 19820 → 19,820"""
    try:
        return "{:,}".format(int(value))
    except (ValueError, TypeError):
        return str(value)

# ─────────────────────────────────────────────────────────────
# PIPELINE LOCK  (concurrency safety)
# ─────────────────────────────────────────────────────────────

_pipeline_lock   = threading.Lock()
_pipeline_running = False
_last_pipeline_report: dict = {}

AUDIT_LOG_FILE = LOGS_DIR / "flask_audit.json"


# ─────────────────────────────────────────────────────────────
# AUDIT LOG HELPERS
# ─────────────────────────────────────────────────────────────

def _write_audit(action: str, entity: str, record_id, old_val=None, new_val=None, pipeline_run_id=None, status="OK"):
    """Append one audit entry to logs/flask_audit.json."""
    ensure_directories()
    log = []
    if AUDIT_LOG_FILE.exists():
        try:
            log = json.loads(AUDIT_LOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            log = []
    log.append({
        "timestamp":       datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "action":          action,
        "entity":          entity,
        "record_id":       record_id,
        "old_value":       old_val,
        "new_value":       new_val,
        "pipeline_run_id": pipeline_run_id,
        "status":          status,
    })
    AUDIT_LOG_FILE.write_text(json.dumps(log, indent=2, default=str), encoding="utf-8")


# ─────────────────────────────────────────────────────────────
# PIPELINE TRIGGER HELPER
# ─────────────────────────────────────────────────────────────

def _trigger_pipeline(db_load: bool = False) -> dict:
    """
    Run run_pipeline() safely.
    Returns the pipeline report dict.
    Raises RuntimeError if pipeline is already running.
    """
    global _pipeline_running, _last_pipeline_report
    if _pipeline_running:
        raise RuntimeError("Pipeline is currently running. Please wait.")
    _pipeline_running = True
    try:
        report = run_pipeline(db_load=db_load)
        _last_pipeline_report = report
        return report
    finally:
        _pipeline_running = False


# ─────────────────────────────────────────────────────────────
# FULL-REFRESH VERIFICATION
# ─────────────────────────────────────────────────────────────

def _verify_counts(report: dict) -> tuple[bool, list[str]]:
    """
    After a pipeline run, verify that:
      Source Product count == FactSales unique product count
      Report detail count  == FactSales row count

    Returns (passed: bool, messages: list[str])
    """
    issues = []
    try:
        # Count source products
        prod_path = SOURCE_FILES["product"]
        src_prod_count = sum(1 for _ in open(prod_path, encoding="utf-8")) if prod_path.exists() else -1

        # Count from pipeline report
        rpt_detail = report.get("detail_source_records", -1)
        rpt_fact   = report.get("factsales_records", -1)

        if rpt_detail != rpt_fact:
            issues.append(
                f"FactSales rows ({rpt_fact:,}) ≠ source detail rows ({rpt_detail:,})"
            )

        # Read kpi_summary for product count cross-check
        kpi_path = DASHBOARD_DIR / "kpi_summary.parquet"
        if kpi_path.exists():
            kpi_df = pd.read_parquet(kpi_path)
            kpi_prod = int(kpi_df["total_products"].iloc[0])
            if kpi_prod != src_prod_count:
                issues.append(
                    f"KPI total_products ({kpi_prod:,}) ≠ source CSV row count ({src_prod_count:,})"
                )
    except Exception as exc:
        issues.append(f"Verification error: {exc}")

    return (len(issues) == 0), issues


def _get_verification_counts(report: dict) -> dict:
    """
    Collects counts across all pipeline stages: Source, Valid Staging, OLTP, OLAP Dim, and OLAP Fact.
    """
    from src.config import STAGING_VALID
    from src.oltp import get_engine
    from sqlalchemy import text

    counts = {
        "source": "N/A",
        "valid": "N/A",
        "oltp": "N/A",
        "dim_product": "N/A",
        "fact_sales": "N/A",
        "status": report.get("status", "UNKNOWN")
    }

    # 1. Source Product Count
    try:
        _, p_rows = _read_source_csv("product")
        counts["source"] = len(p_rows)
    except Exception:
        pass

    # 2. Valid Product Count
    valid_parquet = STAGING_VALID / "valid_product.parquet"
    if valid_parquet.exists():
        try:
            counts["valid"] = len(pd.read_parquet(valid_parquet))
        except Exception:
            pass

    # 3. DB counts (OLTP, DimProduct, FactSales) if connection available
    db_done = False
    engine = get_engine()
    if engine:
        try:
            with engine.connect() as conn:
                counts["oltp"] = conn.execute(text("SELECT COUNT(*) FROM oltp.product")).scalar()
                counts["dim_product"] = conn.execute(text("SELECT COUNT(*) FROM olap.dim_product")).scalar()
                counts["fact_sales"] = conn.execute(text("SELECT COUNT(*) FROM olap.fact_sales")).scalar()
                db_done = True
        except Exception as e:
            print(f"PostgreSQL counts read error: {e}")

    # Fallback to transformed parquet files if DB is not loaded/available
    if not db_done:
        trans_prod = STAGING_VALID / "transformed_product.parquet"
        if trans_prod.exists():
            try:
                prod_df = pd.read_parquet(trans_prod)
                counts["oltp"] = len(prod_df)
                counts["dim_product"] = len(prod_df)
            except Exception:
                pass
        trans_sales = STAGING_VALID / "transformed_sales.parquet"
        if trans_sales.exists():
            try:
                counts["fact_sales"] = len(pd.read_parquet(trans_sales))
            except Exception:
                pass

    return counts


def validate_product_form(form, is_edit=False, existing_products=None) -> list[str]:
    """
    Validates the product form fields according to data engineering rules.
    """
    errors = []

    pid = form.get("ProductID", "").strip()
    name = form.get("Name", "").strip()
    pnum = form.get("ProductNumber", "").strip()
    make_flag = form.get("MakeFlag", "").strip()
    finished_goods_flag = form.get("FinishedGoodsFlag", "").strip()
    safety_stock = form.get("SafetyStockLevel", "").strip()
    reorder_point = form.get("ReorderPoint", "").strip()
    cost = form.get("StandardCost", "").strip()
    price = form.get("ListPrice", "").strip()

    if not is_edit:
        if not pid:
            errors.append("ProductID is required.")
        else:
            try:
                pid_int = int(pid)
                if pid_int <= 0:
                    errors.append("ProductID must be a positive integer.")
                elif existing_products and any(str(r["ProductID"]) == str(pid) for r in existing_products):
                    errors.append(f"ProductID {pid} already exists.")
            except ValueError:
                errors.append("ProductID must be a valid integer.")

    if not name:
        errors.append("Product Name is required.")
    if not pnum:
        errors.append("Product Number is required.")

    if make_flag not in ["0", "1"]:
        errors.append("MakeFlag must be 0 or 1.")
    if finished_goods_flag not in ["0", "1"]:
        errors.append("FinishedGoodsFlag must be 0 or 1.")

    try:
        if int(safety_stock) <= 0:
            errors.append("SafetyStockLevel must be a positive integer.")
    except ValueError:
        errors.append("SafetyStockLevel must be a valid integer.")

    try:
        if int(reorder_point) <= 0:
            errors.append("ReorderPoint must be a positive integer.")
    except ValueError:
        errors.append("ReorderPoint must be a valid integer.")

    try:
        if float(cost) < 0.0:
            errors.append("StandardCost cannot be negative.")
    except ValueError:
        errors.append("StandardCost must be a valid decimal number.")

    try:
        if float(price) < 0.0:
            errors.append("ListPrice cannot be negative.")
    except ValueError:
        errors.append("ListPrice must be a valid decimal number.")

    # Optionals
    weight = form.get("Weight", "").strip()
    if weight:
        try:
            if float(weight) < 0.0:
                errors.append("Weight cannot be negative.")
        except ValueError:
            errors.append("Weight must be a valid decimal number.")

    days = form.get("DaysToManufacture", "").strip()
    if days:
        try:
            if int(days) < 0:
                errors.append("DaysToManufacture cannot be negative.")
        except ValueError:
            errors.append("DaysToManufacture must be a valid integer.")

    return errors



# ─────────────────────────────────────────────────────────────
# DATA READERS  (read from dashboard/ Parquet — no recompute)
# ─────────────────────────────────────────────────────────────

def _read_kpis() -> dict:
    """Read KPI summary from dashboard/kpi_summary.parquet."""
    path = DASHBOARD_DIR / "kpi_summary.parquet"
    if not path.exists():
        return {}
    try:
        df = pd.read_parquet(path)
        return df.iloc[0].to_dict()
    except Exception:
        return {}


def _read_dashboard_file(name: str) -> list[dict]:
    """Read a dashboard CSV by name and return list of row dicts."""
    path = DASHBOARD_DIR / f"{name}.parquet"
    if not path.exists():
        return []
    try:
        df = pd.read_parquet(path)
        return df.fillna("").to_dict(orient="records")
    except Exception:
        return []


def _read_run_history() -> list[dict]:
    """Load pipeline run history from logs/pipeline_runs.json."""
    hist_file = LOGS_DIR / "pipeline_runs.json"
    if not hist_file.exists():
        return []
    try:
        return json.loads(hist_file.read_text(encoding="utf-8"))
    except Exception:
        return []


def _read_dq_results(run_id: str = None) -> list[dict]:
    """
    Load DQ results CSV for the given run_id.
    If run_id is None, loads the most recent one.
    """
    if run_id:
        path = LOGS_DIR / f"dq_results_{run_id}.csv"
        if path.exists():
            df = pd.read_csv(path)
            return df.fillna("").to_dict(orient="records")

    # Find most recent
    files = sorted(LOGS_DIR.glob("dq_results_*.csv"), key=lambda f: f.stat().st_mtime, reverse=True)
    if files:
        df = pd.read_csv(files[0])
        return df.fillna("").to_dict(orient="records")
    return []


def _read_audit_log(limit: int = 50) -> list[dict]:
    """Read audit log, most recent first."""
    if not AUDIT_LOG_FILE.exists():
        return []
    try:
        log = json.loads(AUDIT_LOG_FILE.read_text(encoding="utf-8"))
        return list(reversed(log))[:limit]
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────
# CSV HELPERS  (read/write source CSVs)
# ─────────────────────────────────────────────────────────────

def _read_source_csv(table: str) -> tuple[list[str], list[dict]]:
    """
    Read a source CSV and return (headers, rows).
    Handles tab-separated, no-header format using config columns.
    """
    from src.config import (
        PRODUCT_COLUMNS, CUSTOMER_COLUMNS,
        SALESORDERHEADER_COLUMNS, SALESORDERDETAIL_COLUMNS,
        CSV_SEPARATOR,
    )
    col_map = {
        "product":          PRODUCT_COLUMNS,
        "customer":         CUSTOMER_COLUMNS,
        "salesorderheader": SALESORDERHEADER_COLUMNS,
        "salesorderdetail": SALESORDERDETAIL_COLUMNS,
    }
    path    = SOURCE_FILES[table]
    columns = col_map[table]
    if not path.exists():
        return columns, []
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=CSV_SEPARATOR)
        for raw_row in reader:
            if len(raw_row) == len(columns):
                rows.append(dict(zip(columns, raw_row)))
            elif raw_row:
                # Pad or trim
                padded = (raw_row + [""] * len(columns))[:len(columns)]
                rows.append(dict(zip(columns, padded)))
    return columns, rows


def _write_source_csv(table: str, rows: list[dict], columns: list[str]) -> None:
    """Overwrite a source CSV with the given rows (tab-separated, no header)."""
    from src.config import CSV_SEPARATOR
    path = SOURCE_FILES[table]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=CSV_SEPARATOR)
        for row in rows:
            writer.writerow([row.get(c, "") for c in columns])


# ─────────────────────────────────────────────────────────────
# CONTEXT PROCESSOR  (inject globals into every template)
# ─────────────────────────────────────────────────────────────

@app.context_processor
def inject_globals():
    return {
        "pipeline_running": _pipeline_running,
        "nav_links": [
            ("index",        "Dashboard",    "bi-speedometer2"),
            ("products",     "Products",     "bi-box-seam"),
            ("customers",    "Customers",    "bi-people"),
            ("pipeline",     "Pipeline",     "bi-diagram-3"),
            ("data_quality", "Data Quality", "bi-shield-check"),
            ("analytics",    "Analytics",    "bi-bar-chart-line"),
        ],
    }


# ─────────────────────────────────────────────────────────────
# HOME / DASHBOARD
# ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    kpis    = _read_kpis()
    history = _read_run_history()
    last    = history[-1] if history else {}
    audit   = _read_audit_log(10)

    # Sales by year for sparkline
    sales_by_year = _read_dashboard_file("sales_by_year")

    return render_template(
        "index.html",
        kpis          = kpis,
        last_run      = last,
        audit_log     = audit,
        sales_by_year = sales_by_year,
        pipeline_running = _pipeline_running,
    )


# ─────────────────────────────────────────────────────────────
# PIPELINE STATUS  (AJAX poll)
# ─────────────────────────────────────────────────────────────

@app.route("/api/pipeline/status")
def api_pipeline_status():
    state = {}
    try:
        from src.config import LOGS_DIR
        state_file = LOGS_DIR / "pipeline_state.json"
        if state_file.exists():
            state = json.loads(state_file.read_text(encoding="utf-8"))
    except Exception:
        pass

    return jsonify({
        "running": _pipeline_running,
        "last_report": _last_pipeline_report,
        "state": state,
    })


# ─────────────────────────────────────────────────────────────
# PIPELINE PAGE
# ─────────────────────────────────────────────────────────────

@app.route("/pipeline", methods=["GET"])
def pipeline():
    history = _read_run_history()
    return render_template(
        "pipeline.html",
        history  = list(reversed(history)),
        last_run = history[-1] if history else {},
        pipeline_running = _pipeline_running,
    )


@app.route("/pipeline/run", methods=["POST"])
def pipeline_run():
    global _pipeline_running
    if _pipeline_running:
        flash("Pipeline is currently running. Please wait.", "warning")
        return redirect(url_for("pipeline"))
    is_idempotent = request.form.get("idempotent_test") == "true"
    is_replay = request.form.get("replay_fix") == "true"

    try:
        report = _trigger_pipeline(db_load=False)
        ok, issues = _verify_counts(report)
        if report.get("status") == "SUCCESS" and ok:
            if is_idempotent:
                flash(f"<strong>Idempotency Run SUCCESS!</strong> No data was duplicated. Run ID: {report['run_id']}  |  Duration: {report['duration_sec']}s", "success")
                _write_audit("IDEMPOTENCY_TEST", "pipeline", report.get("run_id"), status="SUCCESS")
            elif is_replay:
                flash(f"<strong>Historical Replay (Backfill) SUCCESS!</strong> Downstream tables updated. Run ID: {report['run_id']}  |  Duration: {report['duration_sec']}s", "success")
                _write_audit("REPLAY_BACKFILL", "pipeline", report.get("run_id"), status="SUCCESS")
            else:
                flash(f"Pipeline SUCCESS — Run ID: {report['run_id']}  |  Duration: {report['duration_sec']}s", "success")
                _write_audit("RUN_PIPELINE", "pipeline", report.get("run_id"), status="SUCCESS")
        elif not ok:
            flash(f"Pipeline completed but COUNT VERIFICATION FAILED: {'; '.join(issues)}", "danger")
            report["status"] = "FAILED"
            _write_audit("RUN_PIPELINE", "pipeline", report.get("run_id"), status="FAILED")
        else:
            flash(f"Pipeline FAILED — {report.get('error_message', 'Unknown error')}  |  Run ID: {report.get('run_id')}", "danger")
            _write_audit("RUN_PIPELINE", "pipeline", report.get("run_id"), status="FAILED")
    except RuntimeError as e:
        flash(str(e), "warning")
    except Exception as e:
        flash(f"Unexpected error: {e}", "danger")
    
    if is_replay:
        return redirect(url_for("audit"))
    return redirect(url_for("pipeline"))


# ─────────────────────────────────────────────────────────────
# PRODUCTS  — View
# ─────────────────────────────────────────────────────────────

@app.route("/products")
def products():
    _, rows = _read_source_csv("product")
    search  = request.args.get("q", "").strip().lower()
    page    = int(request.args.get("page", 1))
    per_page = 25

    if search:
        rows = [r for r in rows if search in r.get("Name", "").lower()
                                or search in r.get("ProductID", "").lower()
                                or search in r.get("Color", "").lower()]

    total   = len(rows)
    pages   = max(1, (total + per_page - 1) // per_page)
    page    = max(1, min(page, pages))
    display = rows[(page - 1) * per_page : page * per_page]

    return render_template(
        "products.html",
        rows     = display,
        page     = page,
        pages    = pages,
        total    = total,
        search   = search,
    )


# ─────────────────────────────────────────────────────────────
# PRODUCTS  — Add
# ─────────────────────────────────────────────────────────────

@app.route("/products/add", methods=["GET", "POST"])
def add_product():
    from src.config import PRODUCT_COLUMNS
    import shutil

    if request.method == "POST":
        form = request.form
        _, existing = _read_source_csv("product")

        # 1. Validation
        errors = validate_product_form(form, is_edit=False, existing_products=existing)
        if errors:
            for err in errors:
                flash(err, "danger")
            return render_template("add_product.html", form=form)

        pid = form.get("ProductID", "").strip()

        # Build new row from ALL columns
        new_row = {col: form.get(col, "").strip() for col in PRODUCT_COLUMNS}
        
        # Auto-fill system fields if missing
        if not new_row.get("rowguid"):
            new_row["rowguid"] = str(uuid.uuid4()).upper()
        if not new_row.get("ModifiedDate"):
            new_row["ModifiedDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        # 2. Backup CSV
        csv_path = SOURCE_FILES["product"]
        backup_path = csv_path.with_suffix(".csv.bak")
        shutil.copy2(csv_path, backup_path)

        # 3. Add record & write
        existing.append(new_row)
        _write_source_csv("product", existing, PRODUCT_COLUMNS)

        # 4. Trigger pipeline
        try:
            report = _trigger_pipeline(db_load=True)
            _write_audit("ADD", "Product", pid, new_val=new_row, 
                         pipeline_run_id=report.get("run_id"), status=report.get("status"))

            if report.get("status") == "SUCCESS":
                # Success: remove backup
                if backup_path.exists():
                    backup_path.unlink()
                
                # Fetch counts report
                counts = _get_verification_counts(report)
                flash(
                    f"<strong>Product added successfully!</strong> Pipeline Run ID: {report['run_id']}<br>"
                    f"📊 Verification Counts:<br>"
                    f"• Source Product Count: {counts['source']}<br>"
                    f"• Valid Product Count: {counts['valid']}<br>"
                    f"• OLTP Product Count: {counts['oltp']}<br>"
                    f"• DimProduct Count: {counts['dim_product']}<br>"
                    f"• FactSales Count: {counts['fact_sales']}<br>"
                    f"• Pipeline Status: <span class='text-success'>{counts['status']}</span>",
                    "success"
                )
                return redirect(url_for("products"))
            else:
                # Failure: restore CSV backup
                if backup_path.exists():
                    shutil.move(str(backup_path), str(csv_path))
                flash(
                    f"<strong>Pipeline FAILED!</strong> Rollback triggered to protect source data consistency.<br>"
                    f"Error: {report.get('error_message')}<br>"
                    f"Run ID: {report.get('run_id')}",
                    "danger"
                )
        except RuntimeError as e:
            if backup_path.exists():
                shutil.move(str(backup_path), str(csv_path))
            flash(str(e), "warning")

    return render_template("add_product.html", form={})


# ─────────────────────────────────────────────────────────────
# PRODUCTS  — Edit
# ─────────────────────────────────────────────────────────────

@app.route("/products/edit/<product_id>", methods=["GET", "POST"])
def edit_product(product_id: str):
    from src.config import PRODUCT_COLUMNS
    import shutil
    columns, rows = _read_source_csv("product")

    target = next((r for r in rows if str(r["ProductID"]) == str(product_id)), None)
    if not target:
        flash(f"Product {product_id} not found.", "danger")
        return redirect(url_for("products"))

    if request.method == "POST":
        form = request.form

        # 1. Validation
        errors = validate_product_form(form, is_edit=True)
        if errors:
            for err in errors:
                flash(err, "danger")
            return render_template("edit_product.html", product=target)

        # 2. Backup CSV
        csv_path = SOURCE_FILES["product"]
        backup_path = csv_path.with_suffix(".csv.bak")
        shutil.copy2(csv_path, backup_path)

        # 3. Update records in memory & save
        old_row = dict(target)
        for col in PRODUCT_COLUMNS:
            if col != "ProductID" and col in form:
                target[col] = form[col].strip()
        
        target["ModifiedDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        _write_source_csv("product", rows, PRODUCT_COLUMNS)

        # 4. Trigger pipeline
        try:
            report = _trigger_pipeline(db_load=True)
            _write_audit("EDIT", "Product", product_id, old_val=old_row, new_val=target,
                         pipeline_run_id=report.get("run_id"), status=report.get("status"))

            if report.get("status") == "SUCCESS":
                # Success: remove backup
                if backup_path.exists():
                    backup_path.unlink()
                
                counts = _get_verification_counts(report)
                flash(
                    f"<strong>Product {product_id} updated!</strong> Pipeline Run ID: {report['run_id']}<br>"
                    f"📊 Verification Counts:<br>"
                    f"• Source Product Count: {counts['source']}<br>"
                    f"• Valid Product Count: {counts['valid']}<br>"
                    f"• OLTP Product Count: {counts['oltp']}<br>"
                    f"• DimProduct Count: {counts['dim_product']}<br>"
                    f"• FactSales Count: {counts['fact_sales']}<br>"
                    f"• Pipeline Status: <span class='text-success'>{counts['status']}</span>",
                    "success"
                )
                return redirect(url_for("products"))
            else:
                # Failure: restore CSV backup
                if backup_path.exists():
                    shutil.move(str(backup_path), str(csv_path))
                flash(
                    f"<strong>Pipeline FAILED!</strong> Rollback triggered to protect source data consistency.<br>"
                    f"Error: {report.get('error_message')}<br>"
                    f"Run ID: {report.get('run_id')}",
                    "danger"
                )
        except RuntimeError as e:
            if backup_path.exists():
                shutil.move(str(backup_path), str(csv_path))
            flash(str(e), "warning")

    return render_template("edit_product.html", product=target)


# ─────────────────────────────────────────────────────────────
# PRODUCTS  — Delete
# ─────────────────────────────────────────────────────────────

@app.route("/products/delete/<product_id>", methods=["POST"])
def delete_product(product_id: str):
    from src.config import PRODUCT_COLUMNS
    import shutil
    columns, rows = _read_source_csv("product")

    old_row = next((r for r in rows if str(r["ProductID"]) == str(product_id)), None)
    if not old_row:
        flash(f"Product {product_id} not found.", "danger")
        return redirect(url_for("products"))

    # 1. Reference check in SalesOrderDetail.csv to prevent data corruption
    _, sales_details = _read_source_csv("salesorderdetail")
    is_referenced = any(str(r["ProductID"]) == str(product_id) for r in sales_details)
    if is_referenced:
        flash(
            f"<strong>Deletion Aborted:</strong> ProductID {product_id} is referenced by historical sales order records.<br>"
            f"Deleting it would violate referential integrity and cause invalid foreign keys in downstream Star Schemas.",
            "danger"
        )
        return redirect(url_for("products"))

    # 2. Backup CSV
    csv_path = SOURCE_FILES["product"]
    backup_path = csv_path.with_suffix(".csv.bak")
    shutil.copy2(csv_path, backup_path)

    # 3. Modify (delete row) & save
    new_rows = [r for r in rows if str(r["ProductID"]) != str(product_id)]
    _write_source_csv("product", new_rows, PRODUCT_COLUMNS)

    # 4. Trigger pipeline
    try:
        report = _trigger_pipeline(db_load=True)
        _write_audit("DELETE", "Product", product_id, old_val=old_row,
                     pipeline_run_id=report.get("run_id"), status=report.get("status"))

        if report.get("status") == "SUCCESS":
            # Success: remove backup
            if backup_path.exists():
                backup_path.unlink()
            
            # Verify that the ProductID no longer exists in downstream parquet
            from src.config import STAGING_VALID
            valid_parquet = STAGING_VALID / "valid_product.parquet"
            verify_msg = ""
            if valid_parquet.exists():
                v_df = pd.read_parquet(valid_parquet)
                still_exists = (v_df["ProductID"].astype(str) == str(product_id)).any()
                if not still_exists:
                    verify_msg = f"<br>✓ Verified ProductID {product_id} is removed from staging Parquet."
                else:
                    verify_msg = f"<br>⚠ Warning: ProductID {product_id} still exists in staging."

            counts = _get_verification_counts(report)
            flash(
                f"<strong>Product {product_id} deleted successfully!</strong>{verify_msg}<br>"
                f"📊 Verification Counts:<br>"
                f"• Source Product Count: {counts['source']}<br>"
                f"• Valid Product Count: {counts['valid']}<br>"
                f"• OLTP Product Count: {counts['oltp']}<br>"
                f"• DimProduct Count: {counts['dim_product']}<br>"
                f"• FactSales Count: {counts['fact_sales']}<br>"
                f"• Pipeline Status: <span class='text-success'>{counts['status']}</span>",
                "success"
            )
        else:
            # Failure: restore CSV backup
            if backup_path.exists():
                shutil.move(str(backup_path), str(csv_path))
            flash(
                f"<strong>Pipeline FAILED!</strong> Rollback triggered to protect source data consistency.<br>"
                f"Error: {report.get('error_message')}<br>"
                f"Run ID: {report.get('run_id')}",
                "danger"
            )
    except RuntimeError as e:
        if backup_path.exists():
            shutil.move(str(backup_path), str(csv_path))
        flash(str(e), "warning")

    return redirect(url_for("products"))



# ─────────────────────────────────────────────────────────────
# CUSTOMERS  — View
# ─────────────────────────────────────────────────────────────

@app.route("/customers")
def customers():
    _, rows  = _read_source_csv("customer")
    search   = request.args.get("q", "").strip().lower()
    page     = int(request.args.get("page", 1))
    per_page = 25

    if search:
        rows = [r for r in rows if search in r.get("CustomerID", "").lower()
                                or search in r.get("AccountNumber", "").lower()]

    total   = len(rows)
    pages   = max(1, (total + per_page - 1) // per_page)
    page    = max(1, min(page, pages))
    display = rows[(page - 1) * per_page : page * per_page]

    return render_template(
        "customers.html",
        rows    = display,
        page    = page,
        pages   = pages,
        total   = total,
        search  = search,
    )


# ─────────────────────────────────────────────────────────────
# CUSTOMERS  — Add
# ─────────────────────────────────────────────────────────────

@app.route("/customers/add", methods=["GET", "POST"])
def add_customer():
    from config import CUSTOMER_COLUMNS
    import shutil
    if request.method == "POST":
        form = request.form
        cid  = form.get("CustomerID", "").strip()
        if not cid:
            flash("CustomerID is required.", "danger")
            return render_template("add_customer.html", form=form)

        _, existing = _read_source_csv("customer")
        if any(str(r["CustomerID"]) == str(cid) for r in existing):
            flash(f"CustomerID {cid} already exists.", "danger")
            return render_template("add_customer.html", form=form)

        new_row = {col: form.get(col, "").strip() for col in CUSTOMER_COLUMNS}
        
        # Auto-fill system fields if missing
        if not new_row.get("rowguid"):
            new_row["rowguid"] = str(uuid.uuid4()).upper()
        if not new_row.get("ModifiedDate"):
            new_row["ModifiedDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        # 2. Backup CSV
        csv_path = SOURCE_FILES["customer"]
        backup_path = csv_path.with_suffix(".csv.bak")
        shutil.copy2(csv_path, backup_path)

        # 3. Add record & write
        existing.append(new_row)
        _write_source_csv("customer", existing, CUSTOMER_COLUMNS)

        # 4. Trigger pipeline
        try:
            report = _trigger_pipeline(db_load=True)
            _write_audit("ADD", "Customer", cid, new_val=new_row,
                         pipeline_run_id=report.get("run_id"), status=report.get("status"))
            
            if report.get("status") == "SUCCESS":
                # Success: remove backup
                if backup_path.exists():
                    backup_path.unlink()
                
                counts = _get_verification_counts(report)
                flash(
                    f"<strong>Customer {cid} added successfully!</strong> Pipeline Run ID: {report['run_id']}<br>"
                    f"📊 Verification Counts:<br>"
                    f"• Source Customer Count: {counts['customer'] if 'customer' in counts else 'N/A'}<br>"
                    f"• Valid Staging Count: {counts['valid']}<br>"
                    f"• OLTP Customer Count: {counts['oltp']}<br>"
                    f"• DimCustomer Count: {counts['dim_customer'] if 'dim_customer' in counts else 'N/A'}<br>"
                    f"• FactSales Count: {counts['fact_sales']}<br>"
                    f"• Pipeline Status: <span class='text-success'>{counts['status']}</span>",
                    "success"
                )
                return redirect(url_for("customers"))
            else:
                # Failure: restore CSV backup
                if backup_path.exists():
                    shutil.move(str(backup_path), str(csv_path))
                flash(
                    f"<strong>Pipeline FAILED!</strong> Rollback triggered to protect source data consistency.<br>"
                    f"Error: {report.get('error_message')}<br>"
                    f"Run ID: {report.get('run_id')}",
                    "danger"
                )
        except RuntimeError as e:
            if backup_path.exists():
                shutil.move(str(backup_path), str(csv_path))
            flash(str(e), "warning")
        except Exception as e:
            if backup_path.exists():
                shutil.move(str(backup_path), str(csv_path))
            app.logger.error(f"Unexpected error adding customer: {e}")
            flash(f"Unexpected Error: {e}", "danger")

    return render_template("add_customer.html", form={})


# ─────────────────────────────────────────────────────────────
# CUSTOMERS  — Edit
# ─────────────────────────────────────────────────────────────

@app.route("/customers/edit/<customer_id>", methods=["GET", "POST"])
def edit_customer(customer_id: str):
    from config import CUSTOMER_COLUMNS
    import shutil
    columns, rows = _read_source_csv("customer")

    target = next((r for r in rows if str(r["CustomerID"]) == str(customer_id)), None)
    if not target:
        flash(f"Customer {customer_id} not found.", "danger")
        return redirect(url_for("customers"))

    if request.method == "POST":
        old_row = dict(target)

        # 2. Backup CSV
        csv_path = SOURCE_FILES["customer"]
        backup_path = csv_path.with_suffix(".csv.bak")
        shutil.copy2(csv_path, backup_path)

        # 3. Update records in memory & save
        editable = ["PersonID", "StoreID", "TerritoryID", "AccountNumber"]
        for f in editable:
            if f in request.form:
                target[f] = request.form[f].strip()

        target["ModifiedDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        _write_source_csv("customer", rows, CUSTOMER_COLUMNS)

        # 4. Trigger pipeline
        try:
            report = _trigger_pipeline(db_load=True)
            _write_audit("EDIT", "Customer", customer_id, old_val=old_row, new_val=target,
                         pipeline_run_id=report.get("run_id"), status=report.get("status"))

            if report.get("status") == "SUCCESS":
                # Success: remove backup
                if backup_path.exists():
                    backup_path.unlink()
                
                counts = _get_verification_counts(report)
                flash(
                    f"<strong>Customer {customer_id} updated!</strong> Pipeline Run ID: {report['run_id']}<br>"
                    f"📊 Verification Counts:<br>"
                    f"• Source Customer Count: {counts['customer'] if 'customer' in counts else 'N/A'}<br>"
                    f"• Valid Staging Count: {counts['valid']}<br>"
                    f"• OLTP Customer Count: {counts['oltp']}<br>"
                    f"• DimCustomer Count: {counts['dim_customer'] if 'dim_customer' in counts else 'N/A'}<br>"
                    f"• FactSales Count: {counts['fact_sales']}<br>"
                    f"• Pipeline Status: <span class='text-success'>{counts['status']}</span>",
                    "success"
                )
                return redirect(url_for("customers"))
            else:
                # Failure: restore CSV backup
                if backup_path.exists():
                    shutil.move(str(backup_path), str(csv_path))
                flash(
                    f"<strong>Pipeline FAILED!</strong> Rollback triggered to protect source data consistency.<br>"
                    f"Error: {report.get('error_message')}<br>"
                    f"Run ID: {report.get('run_id')}",
                    "danger"
                )
        except RuntimeError as e:
            if backup_path.exists():
                shutil.move(str(backup_path), str(csv_path))
            flash(str(e), "warning")
        except Exception as e:
            if backup_path.exists():
                shutil.move(str(backup_path), str(csv_path))
            app.logger.error(f"Unexpected error editing customer: {e}")
            flash(f"Unexpected Error: {e}", "danger")

    return render_template("edit_customer.html", customer=target)


# ─────────────────────────────────────────────────────────────
# CUSTOMERS  — Delete
# ─────────────────────────────────────────────────────────────

@app.route("/customers/delete/<customer_id>", methods=["POST"])
def delete_customer(customer_id: str):
    from config import CUSTOMER_COLUMNS
    import shutil
    columns, rows = _read_source_csv("customer")

    old_row = next((r for r in rows if str(r["CustomerID"]) == str(customer_id)), None)
    if not old_row:
        flash(f"Customer {customer_id} not found.", "danger")
        return redirect(url_for("customers"))

    # 1. Reference check in SalesOrderHeader.csv to prevent data corruption
    _, sales_headers = _read_source_csv("salesorderheader")
    is_referenced = any(str(r["CustomerID"]) == str(customer_id) for r in sales_headers)
    if is_referenced:
        flash(
            f"<strong>Deletion Aborted:</strong> CustomerID {customer_id} is referenced by historical sales order records.<br>"
            f"Deleting it would violate referential integrity and cause invalid foreign keys in downstream Star Schemas.",
            "danger"
        )
        return redirect(url_for("customers"))

    # 2. Backup CSV
    csv_path = SOURCE_FILES["customer"]
    backup_path = csv_path.with_suffix(".csv.bak")
    shutil.copy2(csv_path, backup_path)

    # 3. Modify (delete row) & save
    new_rows = [r for r in rows if str(r["CustomerID"]) != str(customer_id)]
    _write_source_csv("customer", new_rows, CUSTOMER_COLUMNS)

    # 4. Trigger pipeline
    try:
        report = _trigger_pipeline(db_load=True)
        _write_audit("DELETE", "Customer", customer_id, old_val=old_row,
                     pipeline_run_id=report.get("run_id"), status=report.get("status"))

        if report.get("status") == "SUCCESS":
            # Success: remove backup
            if backup_path.exists():
                backup_path.unlink()
            
            counts = _get_verification_counts(report)
            flash(
                f"<strong>Customer {customer_id} deleted successfully!</strong><br>"
                f"📊 Verification Counts:<br>"
                f"• Source Customer Count: {counts['customer'] if 'customer' in counts else 'N/A'}<br>"
                f"• Valid Staging Count: {counts['valid']}<br>"
                f"• OLTP Customer Count: {counts['oltp']}<br>"
                f"• DimCustomer Count: {counts['dim_customer'] if 'dim_customer' in counts else 'N/A'}<br>"
                f"• FactSales Count: {counts['fact_sales']}<br>"
                f"• Pipeline Status: <span class='text-success'>{counts['status']}</span>",
                "success"
            )
        else:
            # Failure: restore CSV backup
            if backup_path.exists():
                shutil.move(str(backup_path), str(csv_path))
            flash(
                f"<strong>Pipeline FAILED!</strong> Rollback triggered to protect source data consistency.<br>"
                f"Error: {report.get('error_message')}<br>"
                f"Run ID: {report.get('run_id')}",
                "danger"
            )
    except RuntimeError as e:
        if backup_path.exists():
            shutil.move(str(backup_path), str(csv_path))
        flash(str(e), "warning")
    except Exception as e:
        if backup_path.exists():
            shutil.move(str(backup_path), str(csv_path))
        app.logger.error(f"Unexpected error deleting customer: {e}")
        flash(f"Unexpected Error: {e}", "danger")

    return redirect(url_for("customers"))


# ─────────────────────────────────────────────────────────────
# DATA QUALITY PAGE
# ─────────────────────────────────────────────────────────────

@app.route("/data-quality")
def data_quality():
    history  = _read_run_history()
    last_run = history[-1] if history else {}
    run_id   = last_run.get("run_id")
    dq_rows  = _read_dq_results(run_id)
    dq_summary_data = _read_dashboard_file("data_quality_summary")

    # Compute summary counts from dq_rows
    total   = len(dq_rows)
    passed  = sum(1 for r in dq_rows if r.get("passed") in (True, "True"))
    warnings= sum(1 for r in dq_rows if not r.get("passed") in (True, "True") and r.get("severity") == "WARNING")
    errors  = sum(1 for r in dq_rows if not r.get("passed") in (True, "True") and r.get("severity") == "ERROR")

    return render_template(
        "data_quality.html",
        dq_rows      = dq_rows,
        dq_summary   = dq_summary_data,
        total_rules  = total,
        passed_count = passed,
        warn_count   = warnings,
        error_count  = errors,
        last_run_id  = run_id,
    )


# ─────────────────────────────────────────────────────────────
# ANALYTICS PAGE
# ─────────────────────────────────────────────────────────────

@app.route("/analytics")
def analytics():
    kpis             = _read_kpis()
    top10            = _read_dashboard_file("top10_products")
    sales_by_month   = _read_dashboard_file("sales_by_month")
    sales_by_year    = _read_dashboard_file("sales_by_year")
    product_by_color = _read_dashboard_file("product_by_color")
    cost_vs_price    = _read_dashboard_file("cost_vs_price")
    sales_by_product = _read_dashboard_file("sales_by_product")

    return render_template(
        "analytics.html",
        kpis             = kpis,
        top10            = top10,
        sales_by_month   = sales_by_month,
        sales_by_year    = sales_by_year,
        product_by_color = product_by_color,
        cost_vs_price    = cost_vs_price,
        sales_by_product = sales_by_product[:20],  # limit for table
    )


# ─────────────────────────────────────────────────────────────
# AUDIT LOG PAGE
# ─────────────────────────────────────────────────────────────

@app.route("/audit")
def audit():
    log = _read_audit_log(100)
    return render_template("audit.html", log=log)


# ─────────────────────────────────────────────────────────────
# CUSTOM ERROR HANDLERS
# ─────────────────────────────────────────────────────────────

@app.errorhandler(404)
def page_not_found(e):
    return render_template("errors/404.html"), 404

@app.errorhandler(500)
def internal_server_error(e):
    import traceback
    app.logger.error(f"Internal Server Error: {e}")
    traceback.print_exc()
    return render_template("errors/500.html"), 500


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000, use_reloader=False)
