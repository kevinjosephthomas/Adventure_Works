# ============================================================
# src/validation.py
# AdventureWorks Data Engineering Pipeline — Step 2
#
# Responsibility:
#   - Schema validation (expected columns present)
#   - Null validation  (key fields must not be NULL)
#   - Duplicate detection (by primary/business key)
#   - Outlier detection (IQR method)
#   - Save valid / invalid / duplicate / outlier records
#     to the appropriate staging folders
# ============================================================

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    STAGING_VALID, STAGING_INVALID, STAGING_DUPLICATES, STAGING_OUTLIERS,
    OUTLIER_IQR_MULTIPLIER, ensure_directories,
)

# ──────────────────────────────────────────────────────────────
# EXPECTED SCHEMAS
# Minimum required columns per table (data columns only).
# ──────────────────────────────────────────────────────────────

EXPECTED_SCHEMAS = {
    "product": [
        "ProductID", "Name", "ProductNumber", "StandardCost", "ListPrice",
        "SellStartDate", "ModifiedDate",
    ],
    "customer": [
        "CustomerID", "TerritoryID", "AccountNumber", "ModifiedDate",
    ],
    "salesorderheader": [
        "SalesOrderID", "OrderDate", "CustomerID", "SubTotal",
        "TaxAmt", "Freight", "TotalDue",
    ],
    "salesorderdetail": [
        "SalesOrderID", "SalesOrderDetailID", "ProductID",
        "OrderQty", "UnitPrice", "UnitPriceDiscount", "LineTotal",
    ],
}

# Primary key columns for null + duplicate checks
PRIMARY_KEYS = {
    "product":          ["ProductID"],
    "customer":         ["CustomerID"],
    "salesorderheader": ["SalesOrderID"],
    "salesorderdetail": ["SalesOrderID", "SalesOrderDetailID"],
}

# Numeric columns to check for outliers
OUTLIER_COLUMNS = {
    "product":          ["StandardCost", "ListPrice", "Weight"],
    "salesorderdetail": ["UnitPrice", "LineTotal"],
}


# ──────────────────────────────────────────────────────────────
# 1. SCHEMA VALIDATION
# ──────────────────────────────────────────────────────────────

def validate_schema(df: pd.DataFrame, table_name: str) -> dict:
    """
    Check that all expected columns are present in the DataFrame.

    Parameters
    ----------
    df         : raw DataFrame
    table_name : one of 'product', 'customer', etc.

    Returns
    -------
    dict with keys: passed (bool), missing_columns (list), message (str)
    """
    expected  = set(EXPECTED_SCHEMAS.get(table_name, []))
    actual    = set(df.columns)
    missing   = sorted(expected - actual)
    extra     = sorted(actual - expected - {"_source_file", "_ingested_at"})

    passed  = len(missing) == 0
    return {
        "table":           table_name,
        "passed":          passed,
        "missing_columns": missing,
        "extra_columns":   extra,
        "message": (
            "Schema OK" if passed
            else f"Missing columns: {missing}"
        ),
    }


# ──────────────────────────────────────────────────────────────
# 2. NULL VALIDATION
# ──────────────────────────────────────────────────────────────

def validate_nulls(
    df: pd.DataFrame,
    table_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Separate records with NULL values in primary key columns.

    Returns
    -------
    valid_df   : rows where all key columns are non-null
    invalid_df : rows where any key column is null
    report     : summary dict
    """
    key_cols = PRIMARY_KEYS.get(table_name, [])

    # Only check columns that actually exist
    key_cols = [c for c in key_cols if c in df.columns]

    if not key_cols:
        return df.copy(), pd.DataFrame(columns=df.columns), {
            "table": table_name, "null_records": 0, "valid_records": len(df),
        }

    null_mask  = df[key_cols].isnull().any(axis=1)
    invalid_df = df[null_mask].copy()
    valid_df   = df[~null_mask].copy()

    if not invalid_df.empty:
        invalid_df["_validation_reason"] = (
            "NULL in key column(s): " + ", ".join(key_cols)
        )

    return valid_df, invalid_df, {
        "table":         table_name,
        "key_columns":   key_cols,
        "null_records":  int(null_mask.sum()),
        "valid_records": int((~null_mask).sum()),
    }


# ──────────────────────────────────────────────────────────────
# 3. DUPLICATE DETECTION
# ──────────────────────────────────────────────────────────────

def validate_duplicates(
    df: pd.DataFrame,
    table_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Detect rows with duplicate primary key values.

    For duplicates: keep the FIRST occurrence in unique_df,
    move all duplicates (keep=False on the duplicate set) to duplicate_df.

    Returns
    -------
    unique_df    : one row per unique key
    duplicate_df : all rows that share a key with another row
    report       : summary dict
    """
    key_cols = PRIMARY_KEYS.get(table_name, [])
    key_cols = [c for c in key_cols if c in df.columns]

    if not key_cols:
        return df.copy(), pd.DataFrame(columns=df.columns), {
            "table": table_name, "duplicate_records": 0,
        }

    dup_mask     = df.duplicated(subset=key_cols, keep=False)
    first_mask   = df.duplicated(subset=key_cols, keep="first")

    duplicate_df = df[first_mask].copy()   # rows beyond the first occurrence
    unique_df    = df[~first_mask].copy()  # keeps first of each duplicate group

    if not duplicate_df.empty:
        duplicate_df["_validation_reason"] = (
            "Duplicate key: " + ", ".join(key_cols)
        )

    return unique_df, duplicate_df, {
        "table":             table_name,
        "key_columns":       key_cols,
        "total_records":     len(df),
        "unique_records":    len(unique_df),
        "duplicate_records": len(duplicate_df),
    }


# ──────────────────────────────────────────────────────────────
# 4. OUTLIER DETECTION  (IQR method)
# ──────────────────────────────────────────────────────────────

def detect_outliers(
    df: pd.DataFrame,
    table_name: str,
    multiplier: float = OUTLIER_IQR_MULTIPLIER,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Flag statistical outliers using the IQR (Inter-Quartile Range) method.

    A value is an outlier if it is below Q1 - multiplier*IQR
    or above Q3 + multiplier*IQR.

    Outliers are RECORDED but NOT removed from valid_df — they
    are stored separately in staging/outliers/ for review.

    Returns
    -------
    df_with_flags : original df with added _is_outlier column
    outlier_df    : rows flagged as outliers (may overlap with valid data)
    report        : per-column outlier summary
    """
    cols = [c for c in OUTLIER_COLUMNS.get(table_name, []) if c in df.columns]

    if not cols:
        df_out = df.copy()
        df_out["_is_outlier"] = False
        return df_out, pd.DataFrame(columns=df.columns), {
            "table": table_name, "columns_checked": [], "outlier_records": 0,
        }

    # Convert columns to numeric (coerce bad values to NaN)
    df_num = df[cols].apply(pd.to_numeric, errors="coerce")

    outlier_flag = pd.Series(False, index=df.index)
    col_reports  = {}

    for col in cols:
        series = df_num[col].dropna()
        if len(series) == 0:
            continue
        q1  = series.quantile(0.25)
        q3  = series.quantile(0.75)
        iqr = q3 - q1
        lo  = q1 - multiplier * iqr
        hi  = q3 + multiplier * iqr

        col_outliers = (df_num[col] < lo) | (df_num[col] > hi)
        outlier_flag = outlier_flag | col_outliers.fillna(False)

        col_reports[col] = {
            "q1": round(q1, 4),
            "q3": round(q3, 4),
            "iqr": round(iqr, 4),
            "lower_bound": round(lo, 4),
            "upper_bound": round(hi, 4),
            "outlier_count": int(col_outliers.sum()),
        }

    df_flagged = df.copy()
    df_flagged["_is_outlier"] = outlier_flag

    outlier_df = df_flagged[outlier_flag].copy()
    if not outlier_df.empty:
        outlier_df["_validation_reason"] = "IQR outlier detected"

    return df_flagged, outlier_df, {
        "table":            table_name,
        "columns_checked":  cols,
        "outlier_records":  int(outlier_flag.sum()),
        "column_details":   col_reports,
    }


# ──────────────────────────────────────────────────────────────
# 5. SAVE VALIDATION OUTPUTS TO STAGING
# ──────────────────────────────────────────────────────────────

def _save_parquet(df: pd.DataFrame, folder: Path, filename: str) -> Path:
    """Save a DataFrame to a Parquet file; skip if df is empty."""
    ensure_directories()
    path = folder / filename
    if not df.empty:
        df.to_parquet(path, index=False, engine="pyarrow")
    return path


# ──────────────────────────────────────────────────────────────
# 6. FULL VALIDATION PIPELINE
# ──────────────────────────────────────────────────────────────

def validate_all(raw: dict) -> dict:
    """
    Run the complete validation pipeline on all raw tables.

    Steps per table:
      1. Schema validation (fail-fast if critical columns missing)
      2. Null validation   (separate invalid records)
      3. Duplicate check   (keep first; store duplicates)
      4. Outlier detection (flag and store; keep in valid set)
      5. Save valid / invalid / duplicate / outlier Parquet files

    Parameters
    ----------
    raw : dict returned by ingestion.extract_all()

    Returns
    -------
    dict with:
      "valid"     : dict of clean DataFrames (ready for transformation)
      "reports"   : per-table validation summaries
      "passed"    : bool — True if all schema checks passed
    """
    ensure_directories()
    valid_data = {}
    reports    = {}
    all_passed = True

    for table_name, df in raw.items():
        print(f"\n  Validating: {table_name} ({len(df):,} records) ...")
        table_report = {"table": table_name}

        # ── Step A: Schema validation ──────────────────────────
        schema_result = validate_schema(df, table_name)
        table_report["schema"] = schema_result
        if not schema_result["passed"]:
            all_passed = False
            print(f"    [SCHEMA FAIL] {schema_result['message']}")
            # Do not stop — record and continue
        else:
            print(f"    [schema] OK")

        # ── Step B: Null validation ────────────────────────────
        after_null, invalid_null, null_report = validate_nulls(df, table_name)
        table_report["nulls"] = null_report
        _save_parquet(invalid_null, STAGING_INVALID, f"invalid_{table_name}_nulls.parquet")
        print(f"    [nulls]  valid={null_report['valid_records']:,}  "
              f"invalid={null_report['null_records']:,}")

        # ── Step C: Duplicate detection ────────────────────────
        after_dedup, dupes_df, dup_report = validate_duplicates(after_null, table_name)
        table_report["duplicates"] = dup_report
        _save_parquet(dupes_df, STAGING_DUPLICATES, f"dup_{table_name}.parquet")
        print(f"    [dupes]  unique={dup_report['unique_records']:,}  "
              f"duplicates={dup_report['duplicate_records']:,}")

        # ── Step D: Outlier detection ──────────────────────────
        flagged_df, outlier_df, outlier_report = detect_outliers(after_dedup, table_name)
        table_report["outliers"] = outlier_report
        _save_parquet(outlier_df, STAGING_OUTLIERS, f"outlier_{table_name}.parquet")
        print(f"    [outliers] {outlier_report['outlier_records']:,} records flagged")

        # ── Save valid records (outlier-flagged rows stay in valid set) ──
        _save_parquet(flagged_df, STAGING_VALID, f"valid_{table_name}.parquet")

        valid_data[table_name] = flagged_df
        reports[table_name]    = table_report

    return {
        "valid":   valid_data,
        "reports": reports,
        "passed":  all_passed,
    }


# ──────────────────────────────────────────────────────────────
# 7. PRINT VALIDATION REPORT
# ──────────────────────────────────────────────────────────────

def print_validation_report(result: dict) -> None:
    """Pretty-print the validation result returned by validate_all()."""
    print("\n" + "=" * 65)
    print("  VALIDATION REPORT")
    print("=" * 65)
    for t, rep in result["reports"].items():
        print(f"\n  TABLE: {t.upper()}")
        print(f"    Schema   : {'OK' if rep['schema']['passed'] else 'FAIL — ' + str(rep['schema']['missing_columns'])}")
        print(f"    Nulls    : {rep['nulls']['null_records']:,} invalid  |  {rep['nulls']['valid_records']:,} valid")
        print(f"    Dupes    : {rep['duplicates']['duplicate_records']:,} duplicates  |  {rep['duplicates']['unique_records']:,} unique")
        print(f"    Outliers : {rep['outliers']['outlier_records']:,} records flagged")

        if rep["outliers"].get("column_details"):
            for col, stats in rep["outliers"]["column_details"].items():
                print(f"      {col}: lo={stats['lower_bound']:,.2f}  "
                      f"hi={stats['upper_bound']:,.2f}  "
                      f"outliers={stats['outlier_count']:,}")

    print()
    status = "PASSED" if result["passed"] else "FAILED"
    print(f"  Overall Schema Status: {status}")
    print("=" * 65)


# ──────────────────────────────────────────────────────────────
# SELF-TEST
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from ingestion import extract_all
    print("Running validation self-test ...\n")
    raw = extract_all()
    result = validate_all(raw)
    print_validation_report(result)
    print("\nValidation self-test complete.")
