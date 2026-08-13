# ============================================================
# src/transformation.py
# AdventureWorks Data Engineering Pipeline — Step 2
#
# Responsibility:
#   - Clean and type-cast Product, Customer, and Sales tables
#   - Join SalesOrderHeader + SalesOrderDetail
#   - Build aggregation datasets
#   - Save transformed data to staging/valid/
# ============================================================

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import STAGING_VALID, ensure_directories


# ──────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ──────────────────────────────────────────────────────────────

def _trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading/trailing whitespace from all string (object) columns."""
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
    return df


def _to_numeric(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Convert columns to numeric; non-parseable values become NaN."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _to_datetime(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Parse date columns; non-parseable values become NaT."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _save(df: pd.DataFrame, filename: str) -> Path:
    """Save a DataFrame to staging/valid/ as Parquet."""
    ensure_directories()
    path = STAGING_VALID / filename
    df.to_parquet(path, index=False, engine="pyarrow")
    return path


# ──────────────────────────────────────────────────────────────
# 1. PRODUCT TRANSFORMATION
# ──────────────────────────────────────────────────────────────

def transform_product(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize the Product table.

    Steps
    -----
    1. Drop rows with NULL ProductID
    2. Remove duplicate ProductID (keep first)
    3. Trim all string fields
    4. Standardize Color (title-case, replace blank with 'N/A')
    5. Convert numeric columns to float/int
    6. Convert date columns to datetime
    7. Add _invalid_price flag for records where ListPrice < 0
    8. Add _transformed_at metadata column
    """
    out = df.copy()

    # Drop metadata columns for clean processing
    meta_cols = [c for c in out.columns if c.startswith("_")]
    data_cols = [c for c in out.columns if not c.startswith("_")]
    out = out[data_cols + meta_cols]

    # 1. Drop NULL ProductID
    before = len(out)
    out = out[out["ProductID"].notna()].copy()
    dropped_null = before - len(out)

    # 2. Deduplicate on ProductID
    before = len(out)
    out = out.drop_duplicates(subset=["ProductID"], keep="first")
    dropped_dup = before - len(out)

    # 3. Trim strings
    out = _trim_strings(out)

    # 4. Standardize Color
    out["Color"] = (
        out["Color"]
        .fillna("N/A")
        .str.strip()
        .str.title()
        .replace({"": "N/A", "Na": "N/A"})
    )

    # 5. Convert numeric columns
    numeric_cols = [
        "ProductID", "MakeFlag", "FinishedGoodsFlag",
        "SafetyStockLevel", "ReorderPoint",
        "StandardCost", "ListPrice", "Weight",
        "DaysToManufacture", "ProductSubcategoryID", "ProductModelID",
    ]
    out = _to_numeric(out, numeric_cols)

    # 6. Convert date columns
    date_cols = ["SellStartDate", "SellEndDate", "DiscontinuedDate", "ModifiedDate"]
    out = _to_datetime(out, date_cols)

    # 7. Flag invalid prices (negative ListPrice or StandardCost)
    out["_invalid_price"] = (
        (out["ListPrice"].fillna(0) < 0) |
        (out["StandardCost"].fillna(0) < 0)
    )

    # 8. Metadata
    out["_transformed_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    print(f"    Product: {len(out):,} clean records  "
          f"(dropped {dropped_null} null-ID, {dropped_dup} duplicates)")
    return out


# ──────────────────────────────────────────────────────────────
# 2. CUSTOMER TRANSFORMATION
# ──────────────────────────────────────────────────────────────

def transform_customer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize the Customer table.

    Steps
    -----
    1. Drop rows with NULL CustomerID
    2. Remove duplicate CustomerID (keep first)
    3. Trim all string fields
    4. Standardize AccountNumber (uppercase, strip)
    5. Convert numeric columns
    6. Convert date columns
    7. Fill optional missing PersonID / StoreID with -1
    """
    out = df.copy()
    meta_cols = [c for c in out.columns if c.startswith("_")]
    data_cols = [c for c in out.columns if not c.startswith("_")]
    out = out[data_cols + meta_cols]

    # 1. Drop NULL CustomerID
    before = len(out)
    out = out[out["CustomerID"].notna()].copy()
    dropped_null = before - len(out)

    # 2. Deduplicate
    before = len(out)
    out = out.drop_duplicates(subset=["CustomerID"], keep="first")
    dropped_dup = before - len(out)

    # 3. Trim strings
    out = _trim_strings(out)

    # 4. Standardize AccountNumber
    if "AccountNumber" in out.columns:
        out["AccountNumber"] = out["AccountNumber"].str.upper().str.strip()

    # 5. Numeric columns
    out = _to_numeric(out, ["CustomerID", "PersonID", "StoreID", "TerritoryID"])

    # 6. Date columns
    out = _to_datetime(out, ["ModifiedDate"])

    # 7. Fill optional NULLs
    out["PersonID"]  = out["PersonID"].fillna(-1).astype(int)
    out["StoreID"]   = out["StoreID"].fillna(-1).astype(int)

    out["_transformed_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    print(f"    Customer: {len(out):,} clean records  "
          f"(dropped {dropped_null} null-ID, {dropped_dup} duplicates)")
    return out


# ──────────────────────────────────────────────────────────────
# 3. SALES TRANSFORMATION  (join Header + Detail)
# ──────────────────────────────────────────────────────────────

def transform_sales(
    df_header: pd.DataFrame,
    df_detail: pd.DataFrame,
) -> pd.DataFrame:
    """
    Join SalesOrderHeader with SalesOrderDetail on SalesOrderID.

    The result is a flat, clean sales dataset with one row per line item.

    Output columns
    --------------
    SalesOrderID, SalesOrderDetailID, ProductID, CustomerID,
    OrderDate, DueDate, ShipDate,
    OrderQty, UnitPrice, UnitPriceDiscount, LineTotal,
    SubTotal, TaxAmt, Freight, TotalDue,
    OnlineOrderFlag, TerritoryID
    """
    # ── Clean Header ──────────────────────────────────────────
    hdr = df_header.copy()
    hdr = _trim_strings(hdr)
    hdr = _to_numeric(hdr, [
        "SalesOrderID", "CustomerID", "TerritoryID", "OnlineOrderFlag",
        "SubTotal", "TaxAmt", "Freight", "TotalDue",
    ])
    hdr = _to_datetime(hdr, ["OrderDate", "DueDate", "ShipDate", "ModifiedDate"])
    hdr = hdr.drop_duplicates(subset=["SalesOrderID"], keep="first")

    # ── Clean Detail ──────────────────────────────────────────
    dtl = df_detail.copy()
    dtl = _trim_strings(dtl)
    dtl = _to_numeric(dtl, [
        "SalesOrderID", "SalesOrderDetailID", "ProductID",
        "OrderQty", "UnitPrice", "UnitPriceDiscount", "LineTotal",
    ])
    dtl = dtl.drop_duplicates(subset=["SalesOrderID", "SalesOrderDetailID"], keep="first")

    # ── Join ──────────────────────────────────────────────────
    header_cols = [
        "SalesOrderID", "CustomerID", "TerritoryID", "OnlineOrderFlag",
        "OrderDate", "DueDate", "ShipDate",
        "SubTotal", "TaxAmt", "Freight", "TotalDue",
    ]
    detail_cols = [
        "SalesOrderID", "SalesOrderDetailID", "ProductID",
        "OrderQty", "UnitPrice", "UnitPriceDiscount", "LineTotal",
    ]

    merged = pd.merge(
        dtl[detail_cols],
        hdr[header_cols],
        on="SalesOrderID",
        how="inner",
    )

    # Drop rows missing critical keys
    merged = merged.dropna(subset=["SalesOrderID", "SalesOrderDetailID",
                                   "ProductID", "CustomerID"])

    merged["_transformed_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    print(f"    Sales (joined): {len(merged):,} line-item records")
    return merged


# ──────────────────────────────────────────────────────────────
# 4. AGGREGATIONS
# ──────────────────────────────────────────────────────────────

def build_aggregations(
    df_sales: pd.DataFrame,
    df_product: pd.DataFrame,
    df_customer: pd.DataFrame,
) -> dict:
    """
    Build all analytics aggregation datasets.

    Returns
    -------
    dict with keys:
      sales_by_product    : total LineTotal + OrderQty per ProductID
      sales_by_customer   : total LineTotal + order count per CustomerID
      avg_product_price   : mean ListPrice per product
      top10_products      : top 10 products by total LineTotal
      sales_by_date       : daily total LineTotal
      sales_by_month      : monthly total LineTotal
      sales_by_year       : yearly total LineTotal
    """
    aggs = {}

    # ── Total sales & quantity by product ────────────────────
    by_prod = (
        df_sales
        .groupby("ProductID", as_index=False)
        .agg(
            TotalSales   = ("LineTotal",  "sum"),
            TotalQty     = ("OrderQty",   "sum"),
            OrderCount   = ("SalesOrderID", "nunique"),
        )
    )
    # Enrich with product name from product table
    prod_lookup = df_product[["ProductID", "Name", "ListPrice", "Color"]].copy()
    prod_lookup["ProductID"] = pd.to_numeric(prod_lookup["ProductID"], errors="coerce")
    by_prod = by_prod.merge(prod_lookup, on="ProductID", how="left")
    aggs["sales_by_product"] = by_prod.sort_values("TotalSales", ascending=False)

    # ── Total sales & orders by customer ─────────────────────
    by_cust = (
        df_sales
        .groupby("CustomerID", as_index=False)
        .agg(
            TotalSales  = ("LineTotal",    "sum"),
            TotalQty    = ("OrderQty",     "sum"),
            OrderCount  = ("SalesOrderID", "nunique"),
        )
    )
    aggs["sales_by_customer"] = by_cust.sort_values("TotalSales", ascending=False)

    # ── Average product ListPrice ─────────────────────────────
    avg_price = df_product[["ProductID", "Name", "ListPrice", "StandardCost"]].copy()
    avg_price["ListPrice"]    = pd.to_numeric(avg_price["ListPrice"],    errors="coerce")
    avg_price["StandardCost"] = pd.to_numeric(avg_price["StandardCost"], errors="coerce")
    avg_price["Margin"]       = avg_price["ListPrice"] - avg_price["StandardCost"]
    aggs["avg_product_price"] = avg_price.sort_values("ListPrice", ascending=False)

    # ── Top 10 products by sales ──────────────────────────────
    aggs["top10_products"] = aggs["sales_by_product"].head(10).reset_index(drop=True)

    # ── Sales by date / month / year ─────────────────────────
    sales_dated = df_sales.dropna(subset=["OrderDate"]).copy()
    sales_dated["OrderDate"]  = pd.to_datetime(sales_dated["OrderDate"], errors="coerce")
    sales_dated["Year"]       = sales_dated["OrderDate"].dt.year
    sales_dated["Month"]      = sales_dated["OrderDate"].dt.month
    sales_dated["YearMonth"]  = sales_dated["OrderDate"].dt.to_period("M").astype(str)
    sales_dated["Date"]       = sales_dated["OrderDate"].dt.date

    aggs["sales_by_date"]  = (
        sales_dated.groupby("Date",      as_index=False)["LineTotal"].sum()
        .rename(columns={"LineTotal": "TotalSales"})
        .sort_values("Date")
    )
    aggs["sales_by_month"] = (
        sales_dated.groupby(["Year", "Month", "YearMonth"], as_index=False)["LineTotal"].sum()
        .rename(columns={"LineTotal": "TotalSales"})
        .sort_values(["Year", "Month"])
    )
    aggs["sales_by_year"]  = (
        sales_dated.groupby("Year", as_index=False)["LineTotal"].sum()
        .rename(columns={"LineTotal": "TotalSales"})
        .sort_values("Year")
    )

    return aggs


# ──────────────────────────────────────────────────────────────
# 5. FULL TRANSFORMATION PIPELINE
# ──────────────────────────────────────────────────────────────

def transform_all(valid_data: dict) -> dict:
    """
    Run all transformations on validated data.

    Parameters
    ----------
    valid_data : dict from validate_all()["valid"]

    Returns
    -------
    dict with:
      "product"           : clean Product DataFrame
      "customer"          : clean Customer DataFrame
      "sales"             : joined + clean Sales DataFrame
      "aggregations"      : dict of aggregation DataFrames
      "staging_paths"     : paths of saved Parquet files
    """
    print("\n  Transforming Product ...")
    df_product  = transform_product(valid_data["product"])
    _save(df_product, "transformed_product.parquet")

    print("  Transforming Customer ...")
    df_customer = transform_customer(valid_data["customer"])
    _save(df_customer, "transformed_customer.parquet")

    print("  Transforming Sales (join Header + Detail) ...")
    df_sales = transform_sales(
        valid_data["salesorderheader"],
        valid_data["salesorderdetail"],
    )
    _save(df_sales, "transformed_sales.parquet")

    print("  Building aggregations ...")
    aggs = build_aggregations(df_sales, df_product, df_customer)

    # Save each aggregation
    for name, agg_df in aggs.items():
        _save(agg_df, f"agg_{name}.parquet")

    print(f"\n  Transformation complete:")
    print(f"    Product records    : {len(df_product):,}")
    print(f"    Customer records   : {len(df_customer):,}")
    print(f"    Sales records      : {len(df_sales):,}")
    print(f"    Aggregation tables : {len(aggs)}")

    return {
        "product":       df_product,
        "customer":      df_customer,
        "sales":         df_sales,
        "aggregations":  aggs,
    }


# ──────────────────────────────────────────────────────────────
# SELF-TEST
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from ingestion import extract_all
    from validation import validate_all
    print("Running transformation self-test ...\n")
    raw    = extract_all()
    val    = validate_all(raw)
    result = transform_all(val["valid"])
    print("\nTop 5 products by sales:")
    top5 = result["aggregations"]["top10_products"][["Name", "TotalSales", "TotalQty"]].head(5)
    print(top5.to_string(index=False))
    print("\nTransformation self-test PASSED.")
