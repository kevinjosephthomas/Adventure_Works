# ============================================================
# src/olap.py
# AdventureWorks Data Engineering Pipeline — Step 2
#
# Responsibility:
#   - Create the Star Schema in PostgreSQL (olap schema)
#   - Build DimDate, DimProduct, DimCustomer, FactSales
#   - Demonstrate OLAP operations using Pandas (no DB required)
#     and SQL queries (when DB is available)
# ============================================================

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, date, timezone, timedelta
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DB_CONNECTION_STRING

try:
    from sqlalchemy import create_engine, text
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False


# ──────────────────────────────────────────────────────────────
# DIM DATE BUILDER  (pure Python — no DB needed)
# ──────────────────────────────────────────────────────────────

def build_dim_date(
    start_date: str = "2019-01-01",
    end_date:   str = "2026-12-31",
) -> pd.DataFrame:
    """
    Build a complete date dimension table covering start_date to end_date.

    Columns:
        date_key, full_date, day_of_month, day_of_week, day_name,
        week_of_year, month_number, month_name, quarter,
        year, year_month, year_quarter, is_weekend, is_weekday
    """
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    df = pd.DataFrame({"full_date": dates})

    df["date_key"]     = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["day_of_month"] = df["full_date"].dt.day.astype("int16")
    df["day_of_week"]  = df["full_date"].dt.dayofweek + 1   # 1=Mon, 7=Sun
    df["day_name"]     = df["full_date"].dt.day_name()
    df["week_of_year"] = df["full_date"].dt.isocalendar().week.astype("int16")
    df["month_number"] = df["full_date"].dt.month.astype("int16")
    df["month_name"]   = df["full_date"].dt.month_name()
    df["quarter"]      = df["full_date"].dt.quarter.astype("int16")
    df["year"]         = df["full_date"].dt.year.astype("int16")
    df["year_month"]   = df["full_date"].dt.strftime("%Y-%m")
    df["year_quarter"] = df["year"].astype(str) + "-Q" + df["quarter"].astype(str)
    df["is_weekend"]   = df["day_of_week"].isin([6, 7])
    df["is_weekday"]   = ~df["is_weekend"]
    df["full_date"]    = df["full_date"].dt.date

    return df


# ──────────────────────────────────────────────────────────────
# DIM PRODUCT BUILDER
# ──────────────────────────────────────────────────────────────

def build_dim_product(df_product: pd.DataFrame) -> pd.DataFrame:
    """
    Build DimProduct from the transformed product DataFrame.

    Adds a surrogate key (product_key) via a simple integer sequence.
    """
    cols = [
        "ProductID", "Name", "ProductNumber", "Color", "ProductLine",
        "Class", "Style", "Size", "StandardCost", "ListPrice",
        "SellStartDate", "SellEndDate",
    ]
    available = [c for c in cols if c in df_product.columns]
    dim = df_product[available].copy().drop_duplicates(subset=["ProductID"])

    # Rename to dimension naming convention
    dim = dim.rename(columns={
        "ProductID":     "product_id",
        "Name":          "name",
        "ProductNumber": "product_number",
        "Color":         "color",
        "ProductLine":   "product_line",
        "Class":         "class",
        "Style":         "style",
        "Size":          "size",
        "StandardCost":  "standard_cost",
        "ListPrice":     "list_price",
        "SellStartDate": "sell_start_date",
        "SellEndDate":   "sell_end_date",
    })

    dim["is_currently_sold"] = dim["sell_end_date"].isna()
    dim["is_outlier"]        = df_product.get("_is_outlier", False)
    dim["product_key"]       = range(1, len(dim) + 1)

    # Put surrogate key first
    key_col  = dim.pop("product_key")
    dim.insert(0, "product_key", key_col)

    return dim.reset_index(drop=True)


# ──────────────────────────────────────────────────────────────
# DIM CUSTOMER BUILDER
# ──────────────────────────────────────────────────────────────

def build_dim_customer(df_customer: pd.DataFrame) -> pd.DataFrame:
    """
    Build DimCustomer from the transformed customer DataFrame.
    """
    cols = ["CustomerID", "AccountNumber", "TerritoryID", "StoreID"]
    available = [c for c in cols if c in df_customer.columns]
    dim = df_customer[available].copy().drop_duplicates(subset=["CustomerID"])

    dim = dim.rename(columns={
        "CustomerID":    "customer_id",
        "AccountNumber": "account_number",
        "TerritoryID":   "territory_id",
        "StoreID":       "store_id",
    })
    dim["customer_key"] = range(1, len(dim) + 1)

    key_col = dim.pop("customer_key")
    dim.insert(0, "customer_key", key_col)

    return dim.reset_index(drop=True)


# ──────────────────────────────────────────────────────────────
# FACT SALES BUILDER
# ──────────────────────────────────────────────────────────────

def build_fact_sales(
    df_sales:    pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_date:    pd.DataFrame,
) -> pd.DataFrame:
    """
    Build FactSales by resolving surrogate keys from all three dimensions.

    Degenerate dimensions: SalesOrderID, SalesOrderDetailID stay in fact.
    """
    fact = df_sales.copy()

    # Ensure correct types for join keys
    fact["ProductID"]  = pd.to_numeric(fact["ProductID"],  errors="coerce")
    fact["CustomerID"] = pd.to_numeric(fact["CustomerID"], errors="coerce")
    fact["OrderDate"]  = pd.to_datetime(fact["OrderDate"],  errors="coerce")

    # Build date_key (YYYYMMDD integer)
    fact["date_key"] = fact["OrderDate"].dt.strftime("%Y%m%d")
    fact["date_key"] = pd.to_numeric(fact["date_key"], errors="coerce")

    # ── Resolve product_key ───────────────────────────────────
    prod_lookup = dim_product[["product_key", "product_id"]].copy()
    prod_lookup["product_id"] = pd.to_numeric(prod_lookup["product_id"], errors="coerce")
    fact = fact.merge(
        prod_lookup.rename(columns={"product_id": "ProductID"}),
        on="ProductID", how="left",
    )

    # ── Resolve customer_key ──────────────────────────────────
    cust_lookup = dim_customer[["customer_key", "customer_id"]].copy()
    cust_lookup["customer_id"] = pd.to_numeric(cust_lookup["customer_id"], errors="coerce")
    fact = fact.merge(
        cust_lookup.rename(columns={"customer_id": "CustomerID"}),
        on="CustomerID", how="left",
    )

    # ── Select FactSales columns ──────────────────────────────
    measure_cols = [
        "product_key", "customer_key", "date_key",
        "SalesOrderID", "SalesOrderDetailID",
        "OrderQty", "UnitPrice", "UnitPriceDiscount", "LineTotal",
        "SubTotal", "TaxAmt", "Freight", "TotalDue",
        "OnlineOrderFlag", "TerritoryID",
    ]
    available = [c for c in measure_cols if c in fact.columns]
    fact = fact[available].copy()

    fact = fact.rename(columns={
        "SalesOrderID":        "sales_order_id",
        "SalesOrderDetailID":  "sales_order_detail_id",
        "OrderQty":            "order_qty",
        "UnitPrice":           "unit_price",
        "UnitPriceDiscount":   "unit_price_discount",
        "LineTotal":           "line_total",
        "SubTotal":            "sub_total",
        "TaxAmt":              "tax_amt",
        "Freight":             "freight",
        "TotalDue":            "total_due",
        "OnlineOrderFlag":     "online_order_flag",
        "TerritoryID":         "territory_id",
    })

    # Drop rows where key resolution failed
    fact = fact.dropna(subset=["product_key", "customer_key", "date_key"])

    # Add surrogate fact_id
    fact.insert(0, "fact_id", range(1, len(fact) + 1))

    return fact.reset_index(drop=True)


# ──────────────────────────────────────────────────────────────
# OLAP OPERATIONS  (Pandas implementations)
# ──────────────────────────────────────────────────────────────

def olap_rollup(fact: pd.DataFrame, dim_date: pd.DataFrame) -> dict:
    """
    Roll-up: Aggregate fact_sales from day -> month -> year.

    Returns dict with three DataFrames: 'daily', 'monthly', 'yearly'.
    """
    # Merge with date dimension
    merged = fact.merge(dim_date[["date_key", "full_date", "year_month", "year"]],
                        on="date_key", how="left")

    daily = (
        merged.groupby("full_date", as_index=False)
        .agg(TotalSales=("line_total","sum"), TotalQty=("order_qty","sum"),
             OrderCount=("sales_order_id","nunique"))
        .sort_values("full_date")
    )

    monthly = (
        merged.groupby("year_month", as_index=False)
        .agg(TotalSales=("line_total","sum"), TotalQty=("order_qty","sum"),
             OrderCount=("sales_order_id","nunique"))
        .sort_values("year_month")
    )

    yearly = (
        merged.groupby("year", as_index=False)
        .agg(TotalSales=("line_total","sum"), TotalQty=("order_qty","sum"),
             OrderCount=("sales_order_id","nunique"),
             UniqueCustomers=("customer_key","nunique"))
        .sort_values("year")
    )

    return {"daily": daily, "monthly": monthly, "yearly": yearly}


def olap_drilldown(fact: pd.DataFrame, dim_date: pd.DataFrame) -> dict:
    """
    Drill-down: Year -> Quarter -> Month -> Day.
    Returns dict with four DataFrames at each granularity.
    """
    merged = fact.merge(
        dim_date[["date_key","full_date","year","quarter","month_number",
                  "month_name","year_quarter","year_month"]],
        on="date_key", how="left",
    )

    year_level    = merged.groupby("year", as_index=False)["line_total"].sum()
    quarter_level = merged.groupby(["year","year_quarter","quarter"], as_index=False)["line_total"].sum().sort_values(["year","quarter"])
    month_level   = merged.groupby(["year","quarter","month_number","year_month"], as_index=False)["line_total"].sum().sort_values(["year","month_number"])
    day_level     = merged.groupby(["year","quarter","month_number","full_date"], as_index=False)["line_total"].sum().sort_values("full_date")

    return {
        "year":    year_level.rename(columns={"line_total":"TotalSales"}),
        "quarter": quarter_level.rename(columns={"line_total":"TotalSales"}),
        "month":   month_level.rename(columns={"line_total":"TotalSales"}),
        "day":     day_level.rename(columns={"line_total":"TotalSales"}),
    }


def olap_slice(
    fact: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_date:    pd.DataFrame,
    color_filter: str = "Black",
) -> pd.DataFrame:
    """
    Slice: fix one dimension value (e.g., color='Black') and show monthly sales.
    """
    prod_colored = dim_product[
        dim_product["color"].str.strip().str.title() == color_filter.title()
    ][["product_key"]].copy()

    sliced = fact[fact["product_key"].isin(prod_colored["product_key"])].copy()
    sliced = sliced.merge(dim_date[["date_key","year_month"]], on="date_key", how="left")

    result = (
        sliced.groupby("year_month", as_index=False)
        .agg(TotalSales=("line_total","sum"), TotalQty=("order_qty","sum"))
        .sort_values("year_month")
    )
    result.attrs["slice_filter"] = f"color = '{color_filter}'"
    return result


def olap_dice(
    fact:         pd.DataFrame,
    dim_product:  pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_date:     pd.DataFrame,
    colors:       list = None,
    year_range:   tuple = None,
    online_only:  bool = False,
) -> pd.DataFrame:
    """
    Dice: apply filters across multiple dimensions simultaneously.
    """
    colors      = colors      or ["Black", "Silver"]
    year_range  = year_range  or (2022, 2024)

    prod_filter = dim_product[
        dim_product["color"].str.strip().str.title().isin([c.title() for c in colors])
    ][["product_key", "name", "color"]]

    date_filter = dim_date[
        (dim_date["year"] >= year_range[0]) & (dim_date["year"] <= year_range[1])
    ][["date_key", "year_month", "year"]]

    result = fact.copy()
    if online_only and "online_order_flag" in result.columns:
        result = result[result["online_order_flag"] == 1]

    result = result.merge(prod_filter,  on="product_key",  how="inner")
    result = result.merge(date_filter,  on="date_key",     how="inner")
    result = result.merge(
        dim_customer[["customer_key", "account_number"]],
        on="customer_key", how="left",
    )

    dice_result = (
        result.groupby(["name", "color", "year_month"], as_index=False)
        .agg(TotalSales=("line_total","sum"), TotalQty=("order_qty","sum"))
        .sort_values("TotalSales", ascending=False)
    )
    return dice_result


# ──────────────────────────────────────────────────────────────
# FULL OLAP BUILD PIPELINE
# ──────────────────────────────────────────────────────────────

def build_olap(transformed: dict) -> dict:
    """
    Build the complete Star Schema in memory.

    Parameters
    ----------
    transformed : dict from transformation.transform_all()

    Returns
    -------
    dict with: dim_date, dim_product, dim_customer, fact_sales
    """
    print("  Building DimDate ...")
    dim_date = build_dim_date()
    print(f"    DimDate: {len(dim_date):,} rows")

    print("  Building DimProduct ...")
    dim_product = build_dim_product(transformed["product"])
    print(f"    DimProduct: {len(dim_product):,} rows")

    print("  Building DimCustomer ...")
    dim_customer = build_dim_customer(transformed["customer"])
    print(f"    DimCustomer: {len(dim_customer):,} rows")

    print("  Building FactSales ...")
    fact_sales = build_fact_sales(
        transformed["sales"], dim_product, dim_customer, dim_date
    )
    print(f"    FactSales: {len(fact_sales):,} rows")

    return {
        "dim_date":     dim_date,
        "dim_product":  dim_product,
        "dim_customer": dim_customer,
        "fact_sales":   fact_sales,
    }


def load_olap_to_db(engine, olap_data: dict) -> dict:
    """
    Load Star Schema tables into PostgreSQL.
    Full refresh: TRUNCATE then INSERT.
    """
    if engine is None:
        print("    [OLAP] No database connection — skipping.")
        return {}

    counts = {}
    load_order = [
        ("dim_date",    "dim_date",     "olap"),
        ("dim_product", "dim_product",  "olap"),
        ("dim_customer","dim_customer", "olap"),
        ("fact_sales",  "fact_sales",   "olap"),
    ]

    try:
        with engine.begin() as conn:
            # Truncate in reverse FK order
            conn.execute(text("TRUNCATE TABLE olap.fact_sales CASCADE"))
            conn.execute(text("TRUNCATE TABLE olap.dim_product CASCADE"))
            conn.execute(text("TRUNCATE TABLE olap.dim_customer CASCADE"))
            conn.execute(text("TRUNCATE TABLE olap.dim_date CASCADE"))

        for key, table, schema in load_order:
            df = olap_data[key]
            # Drop Python metadata cols
            df = df[[c for c in df.columns if not c.startswith("_")]].copy()
            with engine.begin() as conn:
                df.to_sql(table, conn, schema=schema, if_exists="append",
                          index=False, method="multi", chunksize=1000)
            counts[key] = len(df)
            print(f"    [OLAP] {table:<18} : {len(df):,} rows")
    except Exception as e:
        print(f"    [OLAP] Load failed: {e}")

    return counts


# ──────────────────────────────────────────────────────────────
# SELF-TEST
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("OLAP self-test: building DimDate sample ...")
    dim_date = build_dim_date("2022-01-01", "2022-12-31")
    print(dim_date.head(3).to_string(index=False))
    print(f"\nDimDate rows for 2022: {len(dim_date)}")
    print("OLAP self-test PASSED.")
