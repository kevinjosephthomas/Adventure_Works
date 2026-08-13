# ============================================================
# src/data_quality.py
# AdventureWorks Data Engineering Pipeline — Step 4
#
# Responsibility:
#   - Define a configurable Data Quality (DQ) rules engine
#   - Run rules against raw, validated, and transformed data
#   - Classify each rule result as: PASS, WARNING, or ERROR
#   - Aggregate a DQ report with overall pass/fail status
#   - ERROR severity = pipeline should halt; WARNING = continue
# ============================================================

from __future__ import annotations

import pandas as pd
import numpy as np
from dataclasses import dataclass, field, asdict
from typing import Optional
from pathlib import Path
from datetime import datetime, timezone
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))


# ──────────────────────────────────────────────────────────────
# RULE TYPE CONSTANTS
# ──────────────────────────────────────────────────────────────

RULE_ROW_COUNT_MIN    = "ROW_COUNT_MIN"      # table must have >= N rows
RULE_ROW_COUNT_MAX    = "ROW_COUNT_MAX"      # table must have <= N rows
RULE_NULL_RATIO_MAX   = "NULL_RATIO_MAX"     # null% in key col must be < threshold
RULE_DUPE_RATIO_MAX   = "DUPE_RATIO_MAX"     # duplicate% must be < threshold
RULE_OUTLIER_RATIO_MAX = "OUTLIER_RATIO_MAX" # outlier% must be < threshold
RULE_COLUMN_EXISTS    = "COLUMN_EXISTS"      # required column must be present
RULE_TOTAL_SALES_MIN  = "TOTAL_SALES_MIN"    # aggregate sales must exceed threshold


# ──────────────────────────────────────────────────────────────
# DATACLASS: DQRule  — the configuration for one rule
# ──────────────────────────────────────────────────────────────

@dataclass
class DQRule:
    """
    Defines a single data quality rule.

    Fields
    ------
    rule_id     : unique identifier, e.g. 'product_row_count_min'
    table       : target table name, e.g. 'product'
    rule_type   : one of the RULE_* constants above
    threshold   : numeric threshold value
    severity    : 'ERROR' (halt pipeline) or 'WARNING' (log and continue)
    description : human-readable description of what is being checked
    column      : optional — column name for column-specific checks
    """
    rule_id     : str
    table       : str
    rule_type   : str
    threshold   : float
    severity    : str         # 'ERROR' or 'WARNING'
    description : str
    column      : Optional[str] = None


# ──────────────────────────────────────────────────────────────
# DATACLASS: DQResult  — the outcome of running one rule
# ──────────────────────────────────────────────────────────────

@dataclass
class DQResult:
    """
    The outcome of checking one DQRule.

    Fields
    ------
    rule_id       : matches the DQRule.rule_id
    table         : the table being checked
    rule_type     : the type of check
    passed        : True if the check passed
    severity      : 'ERROR' or 'WARNING' (from the rule)
    actual_value  : the actual metric measured
    threshold     : the threshold from the rule
    message       : human-readable result description
    checked_at    : ISO timestamp of when this check ran
    """
    rule_id      : str
    table        : str
    rule_type    : str
    passed       : bool
    severity     : str
    actual_value : float
    threshold    : float
    message      : str
    checked_at   : str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds"))

    @property
    def status_str(self) -> str:
        if self.passed:
            return "PASS"
        return f"FAIL-{self.severity}"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status_str
        return d


# ──────────────────────────────────────────────────────────────
# DEFAULT RULE SET
# These are the rules applied on every pipeline run.
# Adjust thresholds to match your data expectations.
# ──────────────────────────────────────────────────────────────

DEFAULT_RULES: list[DQRule] = [

    # ── Product ───────────────────────────────────────────────
    DQRule(
        rule_id="product_row_count_min",
        table="product",
        rule_type=RULE_ROW_COUNT_MIN,
        threshold=100,
        severity="ERROR",
        description="Product table must have at least 100 rows",
    ),
    DQRule(
        rule_id="product_row_count_max",
        table="product",
        rule_type=RULE_ROW_COUNT_MAX,
        threshold=10_000,
        severity="WARNING",
        description="Product table should not exceed 10,000 rows (unexpected growth)",
    ),
    DQRule(
        rule_id="product_null_ratio",
        table="product",
        rule_type=RULE_NULL_RATIO_MAX,
        threshold=0.01,   # max 1% null ProductIDs
        severity="ERROR",
        description="ProductID must be present in >= 99% of rows",
        column="ProductID",
    ),
    DQRule(
        rule_id="product_dupe_ratio",
        table="product",
        rule_type=RULE_DUPE_RATIO_MAX,
        threshold=0.005,  # max 0.5% duplicates
        severity="WARNING",
        description="ProductID duplicates must be < 0.5% of rows",
        column="ProductID",
    ),
    DQRule(
        rule_id="product_outlier_ratio",
        table="product",
        rule_type=RULE_OUTLIER_RATIO_MAX,
        threshold=0.25,   # max 25% outlier prices
        severity="WARNING",
        description="Price/Weight outliers should be < 25% of products",
    ),
    DQRule(
        rule_id="product_name_exists",
        table="product",
        rule_type=RULE_COLUMN_EXISTS,
        threshold=1.0,
        severity="ERROR",
        description="Product.Name column must exist in source data",
        column="Name",
    ),

    # ── Customer ──────────────────────────────────────────────
    DQRule(
        rule_id="customer_row_count_min",
        table="customer",
        rule_type=RULE_ROW_COUNT_MIN,
        threshold=1_000,
        severity="ERROR",
        description="Customer table must have at least 1,000 rows",
    ),
    DQRule(
        rule_id="customer_null_ratio",
        table="customer",
        rule_type=RULE_NULL_RATIO_MAX,
        threshold=0.001,
        severity="ERROR",
        description="CustomerID must be present in >= 99.9% of rows",
        column="CustomerID",
    ),
    DQRule(
        rule_id="customer_dupe_ratio",
        table="customer",
        rule_type=RULE_DUPE_RATIO_MAX,
        threshold=0.001,
        severity="ERROR",
        description="CustomerID duplicates must be < 0.1%",
        column="CustomerID",
    ),

    # ── Sales Order Header ────────────────────────────────────
    DQRule(
        rule_id="header_row_count_min",
        table="salesorderheader",
        rule_type=RULE_ROW_COUNT_MIN,
        threshold=1_000,
        severity="ERROR",
        description="SalesOrderHeader must have at least 1,000 orders",
    ),
    DQRule(
        rule_id="header_null_ratio",
        table="salesorderheader",
        rule_type=RULE_NULL_RATIO_MAX,
        threshold=0.001,
        severity="ERROR",
        description="SalesOrderID must be present in >= 99.9% of rows",
        column="SalesOrderID",
    ),
    DQRule(
        rule_id="header_dupe_ratio",
        table="salesorderheader",
        rule_type=RULE_DUPE_RATIO_MAX,
        threshold=0.001,
        severity="ERROR",
        description="SalesOrderID must be unique (< 0.1% duplicates)",
        column="SalesOrderID",
    ),

    # ── Sales Order Detail ────────────────────────────────────
    DQRule(
        rule_id="detail_row_count_min",
        table="salesorderdetail",
        rule_type=RULE_ROW_COUNT_MIN,
        threshold=10_000,
        severity="ERROR",
        description="SalesOrderDetail must have at least 10,000 line items",
    ),
    DQRule(
        rule_id="detail_null_ratio",
        table="salesorderdetail",
        rule_type=RULE_NULL_RATIO_MAX,
        threshold=0.001,
        severity="ERROR",
        description="SalesOrderDetailID must be present in >= 99.9% of rows",
        column="SalesOrderDetailID",
    ),
    DQRule(
        rule_id="detail_outlier_ratio",
        table="salesorderdetail",
        rule_type=RULE_OUTLIER_RATIO_MAX,
        threshold=0.20,   # max 20% outlier line totals
        severity="WARNING",
        description="LineTotal outliers should be < 20% of detail rows",
    ),

    # ── Business-level rule (cross-table) ─────────────────────
    DQRule(
        rule_id="total_sales_min",
        table="salesorderdetail",
        rule_type=RULE_TOTAL_SALES_MIN,
        threshold=1_000_000,   # total sales must exceed $1M
        severity="ERROR",
        description="Total LineTotal across all orders must exceed $1,000,000",
        column="LineTotal",
    ),
]


# ──────────────────────────────────────────────────────────────
# RULE EVALUATORS
# ──────────────────────────────────────────────────────────────

def _check_row_count_min(df: pd.DataFrame, rule: DQRule) -> DQResult:
    actual = len(df)
    passed = actual >= rule.threshold
    return DQResult(
        rule_id=rule.rule_id, table=rule.table, rule_type=rule.rule_type,
        passed=passed, severity=rule.severity,
        actual_value=actual, threshold=rule.threshold,
        message=f"Row count {actual:,} {'>=': <2} {rule.threshold:,.0f} — {'OK' if passed else 'FAIL'}",
    )


def _check_row_count_max(df: pd.DataFrame, rule: DQRule) -> DQResult:
    actual = len(df)
    passed = actual <= rule.threshold
    return DQResult(
        rule_id=rule.rule_id, table=rule.table, rule_type=rule.rule_type,
        passed=passed, severity=rule.severity,
        actual_value=actual, threshold=rule.threshold,
        message=f"Row count {actual:,} {'<=': <2} {rule.threshold:,.0f} — {'OK' if passed else 'FAIL'}",
    )


def _check_null_ratio(df: pd.DataFrame, rule: DQRule) -> DQResult:
    col = rule.column
    if col not in df.columns:
        return DQResult(
            rule_id=rule.rule_id, table=rule.table, rule_type=rule.rule_type,
            passed=False, severity=rule.severity,
            actual_value=-1, threshold=rule.threshold,
            message=f"Column '{col}' not found in table — cannot check null ratio",
        )
    null_count  = df[col].isna().sum()
    null_ratio  = null_count / len(df) if len(df) > 0 else 0.0
    passed      = null_ratio <= rule.threshold
    return DQResult(
        rule_id=rule.rule_id, table=rule.table, rule_type=rule.rule_type,
        passed=passed, severity=rule.severity,
        actual_value=round(null_ratio, 6), threshold=rule.threshold,
        message=f"Null ratio for '{col}': {null_ratio*100:.3f}% (threshold {rule.threshold*100:.2f}%) — {'OK' if passed else 'FAIL'}",
    )


def _check_dupe_ratio(df: pd.DataFrame, rule: DQRule) -> DQResult:
    col = rule.column
    if col not in df.columns:
        return DQResult(
            rule_id=rule.rule_id, table=rule.table, rule_type=rule.rule_type,
            passed=False, severity=rule.severity,
            actual_value=-1, threshold=rule.threshold,
            message=f"Column '{col}' not found — cannot check duplicate ratio",
        )
    dupe_count  = df.duplicated(subset=[col], keep="first").sum()
    dupe_ratio  = dupe_count / len(df) if len(df) > 0 else 0.0
    passed      = dupe_ratio <= rule.threshold
    return DQResult(
        rule_id=rule.rule_id, table=rule.table, rule_type=rule.rule_type,
        passed=passed, severity=rule.severity,
        actual_value=round(dupe_ratio, 6), threshold=rule.threshold,
        message=f"Duplicate ratio for '{col}': {dupe_ratio*100:.4f}% (threshold {rule.threshold*100:.3f}%) — {'OK' if passed else 'FAIL'}",
    )


def _check_outlier_ratio(df: pd.DataFrame, rule: DQRule, val_report: dict) -> DQResult:
    table_rep     = val_report.get(rule.table, {})
    outlier_rep   = table_rep.get("outliers", {})
    outlier_count = outlier_rep.get("outlier_records", 0)
    total         = len(df)
    ratio         = outlier_count / total if total > 0 else 0.0
    passed        = ratio <= rule.threshold
    return DQResult(
        rule_id=rule.rule_id, table=rule.table, rule_type=rule.rule_type,
        passed=passed, severity=rule.severity,
        actual_value=round(ratio, 6), threshold=rule.threshold,
        message=f"Outlier ratio: {ratio*100:.2f}% ({outlier_count:,} records) — threshold {rule.threshold*100:.1f}% — {'OK' if passed else 'FAIL'}",
    )


def _check_column_exists(df: pd.DataFrame, rule: DQRule) -> DQResult:
    col    = rule.column
    passed = col in df.columns
    return DQResult(
        rule_id=rule.rule_id, table=rule.table, rule_type=rule.rule_type,
        passed=passed, severity=rule.severity,
        actual_value=1.0 if passed else 0.0, threshold=1.0,
        message=f"Column '{col}' {'found' if passed else 'NOT FOUND'} in table",
    )


def _check_total_sales_min(df: pd.DataFrame, rule: DQRule) -> DQResult:
    col = rule.column or "LineTotal"
    if col not in df.columns:
        return DQResult(
            rule_id=rule.rule_id, table=rule.table, rule_type=rule.rule_type,
            passed=False, severity=rule.severity,
            actual_value=0.0, threshold=rule.threshold,
            message=f"Column '{col}' not found — cannot check total sales",
        )
    total  = pd.to_numeric(df[col], errors="coerce").sum()
    passed = total >= rule.threshold
    return DQResult(
        rule_id=rule.rule_id, table=rule.table, rule_type=rule.rule_type,
        passed=passed, severity=rule.severity,
        actual_value=round(float(total), 2), threshold=rule.threshold,
        message=f"Total {col}: ${total:,.2f} (threshold ${rule.threshold:,.2f}) — {'OK' if passed else 'FAIL'}",
    )


# ──────────────────────────────────────────────────────────────
# MAIN ENGINE: run_data_quality_checks
# ──────────────────────────────────────────────────────────────

def run_data_quality_checks(
    raw:        dict,
    val_report: dict,
    rules:      list[DQRule] = None,
) -> tuple[list[DQResult], dict]:
    """
    Evaluate all DQ rules against the raw source DataFrames.

    Parameters
    ----------
    raw        : dict of {table_name: DataFrame} from extract_all()
    val_report : validation report dict from validate_all()["reports"]
    rules      : list of DQRule — defaults to DEFAULT_RULES

    Returns
    -------
    results    : list[DQResult] — one per rule
    summary    : dict with overall counts and pass/fail status
    """
    rules   = rules or DEFAULT_RULES
    results = []

    for rule in rules:
        df = raw.get(rule.table)
        if df is None:
            results.append(DQResult(
                rule_id=rule.rule_id, table=rule.table, rule_type=rule.rule_type,
                passed=False, severity="ERROR",
                actual_value=-1, threshold=rule.threshold,
                message=f"Table '{rule.table}' not found in data",
            ))
            continue

        try:
            if rule.rule_type == RULE_ROW_COUNT_MIN:
                results.append(_check_row_count_min(df, rule))
            elif rule.rule_type == RULE_ROW_COUNT_MAX:
                results.append(_check_row_count_max(df, rule))
            elif rule.rule_type == RULE_NULL_RATIO_MAX:
                results.append(_check_null_ratio(df, rule))
            elif rule.rule_type == RULE_DUPE_RATIO_MAX:
                results.append(_check_dupe_ratio(df, rule))
            elif rule.rule_type == RULE_OUTLIER_RATIO_MAX:
                results.append(_check_outlier_ratio(df, rule, val_report))
            elif rule.rule_type == RULE_COLUMN_EXISTS:
                results.append(_check_column_exists(df, rule))
            elif rule.rule_type == RULE_TOTAL_SALES_MIN:
                results.append(_check_total_sales_min(df, rule))
            else:
                results.append(DQResult(
                    rule_id=rule.rule_id, table=rule.table, rule_type=rule.rule_type,
                    passed=False, severity="ERROR",
                    actual_value=-1, threshold=rule.threshold,
                    message=f"Unknown rule type: {rule.rule_type}",
                ))
        except Exception as exc:
            results.append(DQResult(
                rule_id=rule.rule_id, table=rule.table, rule_type=rule.rule_type,
                passed=False, severity="ERROR",
                actual_value=-1, threshold=rule.threshold,
                message=f"Rule evaluation error: {exc}",
            ))

    # ── Build summary ──────────────────────────────────────────
    pass_count  = sum(1 for r in results if r.passed)
    warn_count  = sum(1 for r in results if not r.passed and r.severity == "WARNING")
    error_count = sum(1 for r in results if not r.passed and r.severity == "ERROR")
    total       = len(results)
    all_pass    = error_count == 0

    summary = {
        "total_rules":    total,
        "passed":         pass_count,
        "warnings":       warn_count,
        "errors":         error_count,
        "overall_status": "PASS" if all_pass else "FAIL",
        "halt_pipeline":  error_count > 0,
    }

    return results, summary


# ──────────────────────────────────────────────────────────────
# PRINT DQ REPORT
# ──────────────────────────────────────────────────────────────

def print_dq_report(results: list[DQResult], summary: dict) -> None:
    """Print a formatted data quality report."""
    print("\n" + "=" * 65)
    print("  DATA QUALITY REPORT")
    print("=" * 65)
    for r in results:
        tag = "[PASS   ]" if r.passed else f"[{r.status_str:<8}]"
        print(f"  {tag}  {r.rule_id:<35}  {r.message}")
    print()
    print(f"  Total Rules : {summary['total_rules']}")
    print(f"  Passed      : {summary['passed']}")
    print(f"  Warnings    : {summary['warnings']}")
    print(f"  Errors      : {summary['errors']}")
    print(f"  Overall     : {summary['overall_status']}")
    if summary["halt_pipeline"]:
        print("\n  !! ERROR-level rules failed — pipeline should HALT !!")
    print("=" * 65)


# ──────────────────────────────────────────────────────────────
# SAVE DQ RESULTS TO CSV (for history)
# ──────────────────────────────────────────────────────────────

def save_dq_results(results: list[DQResult], run_id: str, logs_dir: Path) -> Path:
    """Save DQ results to a CSV file in logs/ for audit trail."""
    df = pd.DataFrame([r.to_dict() for r in results])
    df["run_id"] = run_id
    out_path = logs_dir / f"dq_results_{run_id}.csv"
    df.to_csv(out_path, index=False)
    return out_path


# ──────────────────────────────────────────────────────────────
# SELF-TEST
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from ingestion  import extract_all
    from validation import validate_all

    print("Running DQ self-test ...\n")
    raw        = extract_all()
    val        = validate_all(raw)
    results, summary = run_data_quality_checks(raw, val["reports"])
    print_dq_report(results, summary)
    print(f"\nDQ self-test: {summary['overall_status']}")
