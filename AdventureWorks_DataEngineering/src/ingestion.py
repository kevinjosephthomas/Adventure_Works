# ============================================================
# src/ingestion.py
# AdventureWorks Data Engineering Pipeline — Step 1
#
# Responsibility:
#   - Read the four source CSV files using Pandas
#   - Assign correct column names (files have no header)
#   - Save raw copies to staging/raw/
#   - Return DataFrames for downstream use
# ============================================================

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

# Import all settings from the single config file
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (
    SOURCE_FILES,
    CSV_SEPARATOR,
    STAGING_RAW,
    PRODUCT_COLUMNS,
    CUSTOMER_COLUMNS,
    SALESORDERHEADER_COLUMNS,
    SALESORDERDETAIL_COLUMNS,
    ensure_directories,
)


# ──────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ──────────────────────────────────────────────────────────────

def _read_csv(name: str, columns: list[str]) -> pd.DataFrame:
    """
    Read a single AdventureWorks CSV (tab-separated, no header).

    Parameters
    ----------
    name    : logical name, e.g. "product"
    columns : ordered list of column names to assign

    Returns
    -------
    pd.DataFrame with correct column names and an extra
    _source_file / _ingested_at metadata columns.
    """
    path = SOURCE_FILES[name]

    if not path.exists():
        raise FileNotFoundError(
            f"Source file NOT FOUND: {path}\n"
            f"  → Open src/config.py and update DATA_PATH to point to your CSVs."
        )

    df = pd.read_csv(
        path,
        sep=CSV_SEPARATOR,
        header=None,           # Files have no header row
        names=columns,         # Assign column names positionally
        low_memory=False,
        encoding="utf-8",
        on_bad_lines="warn",   # Log bad lines but do not crash
    )

    # Metadata columns — useful for lineage tracing
    df["_source_file"]  = str(path)
    df["_ingested_at"]  = datetime.now(timezone.utc).isoformat(timespec="seconds")

    return df


def _save_raw(df: pd.DataFrame, table_name: str) -> Path:
    """
    Persist a raw DataFrame to staging/raw/ as a Parquet file.
    Parquet preserves dtypes and is fast to read downstream.

    Returns the path of the saved file.
    """
    ensure_directories()
    out_path = STAGING_RAW / f"{table_name}.parquet"
    df.to_parquet(out_path, index=False, engine="pyarrow")
    return out_path


# ──────────────────────────────────────────────────────────────
# PUBLIC EXTRACTION FUNCTIONS
# ──────────────────────────────────────────────────────────────

def extract_product() -> pd.DataFrame:
    """Extract Product.csv and return a raw DataFrame."""
    return _read_csv("product", PRODUCT_COLUMNS)


def extract_customer() -> pd.DataFrame:
    """Extract Customer.csv and return a raw DataFrame."""
    return _read_csv("customer", CUSTOMER_COLUMNS)


def extract_salesorderheader() -> pd.DataFrame:
    """Extract SalesOrderHeader.csv and return a raw DataFrame."""
    return _read_csv("salesorderheader", SALESORDERHEADER_COLUMNS)


def extract_salesorderdetail() -> pd.DataFrame:
    """Extract SalesOrderDetail.csv and return a raw DataFrame."""
    return _read_csv("salesorderdetail", SALESORDERDETAIL_COLUMNS)


# ──────────────────────────────────────────────────────────────
# FULL EXTRACTION — called by the pipeline
# ──────────────────────────────────────────────────────────────

def extract_all() -> dict[str, pd.DataFrame]:
    """
    Extract all four source tables and return them as a dictionary.

    Usage
    -----
    raw = extract_all()
    raw["product"]          # Product DataFrame
    raw["customer"]         # Customer DataFrame
    raw["salesorderheader"] # SalesOrderHeader DataFrame
    raw["salesorderdetail"] # SalesOrderDetail DataFrame
    """
    return {
        "product":            extract_product(),
        "customer":           extract_customer(),
        "salesorderheader":   extract_salesorderheader(),
        "salesorderdetail":   extract_salesorderdetail(),
    }


# ──────────────────────────────────────────────────────────────
# RAW STAGING — called by the pipeline after extraction
# ──────────────────────────────────────────────────────────────

def stage_raw(raw: dict[str, pd.DataFrame]) -> dict[str, Path]:
    """
    Write every extracted DataFrame to staging/raw/ as Parquet.

    This is a FULL REFRESH write: the file is always overwritten,
    so staging/raw/ always reflects the current source state.

    Parameters
    ----------
    raw : dict returned by extract_all()

    Returns
    -------
    dict mapping table name → saved file path
    """
    paths = {}
    table_map = {
        "product":            "raw_product",
        "customer":           "raw_customer",
        "salesorderheader":   "raw_salesorderheader",
        "salesorderdetail":   "raw_salesorderdetail",
    }
    for key, table_name in table_map.items():
        paths[key] = _save_raw(raw[key], table_name)
    return paths


# ──────────────────────────────────────────────────────────────
# DISPLAY HELPERS (used in notebooks)
# ──────────────────────────────────────────────────────────────

def display_summary(raw: dict[str, pd.DataFrame]) -> None:
    """
    Print a human-readable summary of all extracted tables:
      - source file path
      - number of records
      - number of columns
      - column names + dtypes (schema)
      - first 3 sample records
    """
    labels = {
        "product":            "Product",
        "customer":           "Customer",
        "salesorderheader":   "SalesOrderHeader",
        "salesorderdetail":   "SalesOrderDetail",
    }

    for key, df in raw.items():
        label = labels[key]
        print("\n" + "=" * 70)
        print(f"  TABLE: {label}")
        print("=" * 70)
        print(f"  Source File : {SOURCE_FILES[key]}")
        print(f"  Records     : {len(df):,}")
        print(f"  Columns     : {len(df.columns)}")
        print()
        print("  SCHEMA (column name -> dtype):")
        print("  " + "-" * 46)
        for col, dtype in df.dtypes.items():
            if not col.startswith("_"):          # skip internal metadata cols
                print(f"    {col:<35s} {str(dtype)}")
        print()
        print("  SAMPLE RECORDS (first 3):")
        # Show only the data columns (exclude internal metadata)
        data_cols = [c for c in df.columns if not c.startswith("_")]
        print(df[data_cols].head(3).to_string(index=False))
        print()


# ──────────────────────────────────────────────────────────────
# QUICK SELF-TEST
# Run: python ingestion.py
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Running ingestion self-test …\n")
    raw = extract_all()
    display_summary(raw)
    paths = stage_raw(raw)
    print("\nRaw staging files written:")
    for k, p in paths.items():
        print(f"  {k:20s} -> {p}")
    print("\nIngestion self-test PASSED.")
