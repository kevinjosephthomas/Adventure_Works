# ============================================================
# src/analytics.py
# AdventureWorks Data Engineering Pipeline — Step 3
#
# Responsibility:
#   - Build final dashboard-ready datasets from transformed data
#   - Compute KPI summary values
#   - Save all outputs to dashboard/ folder as Parquet + CSV
#   - Also used by the pipeline report
# ============================================================

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DASHBOARD_DIR, STAGING_VALID, ensure_directories


# ──────────────────────────────────────────────────────────────
# SAVE HELPER
# ──────────────────────────────────────────────────────────────

def _save(df: pd.DataFrame, name: str) -> Path:
    """Save a DataFrame as both Parquet and CSV to dashboard/."""
    ensure_directories()
    parquet_path = DASHBOARD_DIR / f"{name}.parquet"
    csv_path     = DASHBOARD_DIR / f"{name}.csv"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    df.to_csv(csv_path, index=False)
    return parquet_path


# ──────────────────────────────────────────────────────────────
# KPI SUMMARY
# ──────────────────────────────────────────────────────────────

def compute_kpis(
    transformed: dict,
    val_report:  dict,
    olap_data:   dict = None,
) -> dict:
    """
    Compute all 11 KPIs defined in Exercise 5.

    KPIs:
     1.  Total Products
     2.  Total Customers
     3.  Total Orders
     4.  Total Sales
     5.  Total Quantity Sold
     6.  Average Product Price
     7.  Average Order Value
     8.  Invalid Records
     9.  Duplicate Records
     10. Outlier Records
     11. Pipeline Status
    """
    df_prod   = transformed["product"]
    df_cust   = transformed["customer"]
    df_sales  = transformed["sales"]

    total_sales      = df_sales["LineTotal"].sum()
    total_qty        = df_sales["OrderQty"].sum()
    total_orders     = df_sales["SalesOrderID"].nunique()
    avg_order_value  = df_sales.groupby("SalesOrderID")["LineTotal"].sum().mean()

    list_prices      = pd.to_numeric(df_prod["ListPrice"], errors="coerce")
    avg_list_price   = list_prices[list_prices > 0].mean()

    # Validation counts from report
    total_invalid  = sum(
        r["nulls"]["null_records"] for r in val_report.values()
    )
    total_dupes    = sum(
        r["duplicates"]["duplicate_records"] for r in val_report.values()
    )
    total_outliers = sum(
        r["outliers"]["outlier_records"] for r in val_report.values()
    )

    return {
        "total_products":     len(df_prod),
        "total_customers":    len(df_cust),
        "total_orders":       int(total_orders),
        "total_sales":        round(float(total_sales), 2),
        "total_qty_sold":     int(total_qty),
        "avg_list_price":     round(float(avg_list_price), 2),
        "avg_order_value":    round(float(avg_order_value), 2),
        "invalid_records":    int(total_invalid),
        "duplicate_records":  int(total_dupes),
        "outlier_records":    int(total_outliers),
        "pipeline_status":    "SUCCESS",   # updated by pipeline.py if needed
    }


# ──────────────────────────────────────────────────────────────
# CHART DATASETS
# ──────────────────────────────────────────────────────────────

def build_dashboard_datasets(transformed: dict, olap_data: dict = None) -> dict:
    """
    Build all chart-ready datasets and save to dashboard/.

    Returns a dict of dataset_name -> DataFrame.
    """
    ensure_directories()
    datasets = {}
    df_sales  = transformed["sales"]
    df_prod   = transformed["product"]
    df_cust   = transformed["customer"]

    # ── 1. Top 10 Products by Sales ──────────────────────────
    by_prod = (
        df_sales.groupby("ProductID", as_index=False)
        .agg(TotalSales=("LineTotal","sum"), TotalQty=("OrderQty","sum"))
    )
    prod_lookup = df_prod[["ProductID","Name","Color","ListPrice"]].copy()
    prod_lookup["ProductID"] = pd.to_numeric(prod_lookup["ProductID"], errors="coerce")
    by_prod = by_prod.merge(prod_lookup, on="ProductID", how="left")
    top10 = by_prod.nlargest(10, "TotalSales").reset_index(drop=True)
    datasets["top10_products"] = top10
    _save(top10, "top10_products")

    # ── 2. Sales by Product (all) ─────────────────────────────
    datasets["sales_by_product"] = by_prod.sort_values("TotalSales", ascending=False)
    _save(datasets["sales_by_product"], "sales_by_product")

    # ── 3. Sales by Customer (top 50) ────────────────────────
    by_cust = (
        df_sales.groupby("CustomerID", as_index=False)
        .agg(TotalSales=("LineTotal","sum"), OrderCount=("SalesOrderID","nunique"))
    )
    datasets["sales_by_customer"] = by_cust.nlargest(50, "TotalSales").reset_index(drop=True)
    _save(datasets["sales_by_customer"], "sales_by_customer")

    # ── 4. Sales by Month ─────────────────────────────────────
    sales_dated = df_sales.copy()
    sales_dated["OrderDate"] = pd.to_datetime(sales_dated["OrderDate"], errors="coerce")
    sales_dated = sales_dated.dropna(subset=["OrderDate"])
    sales_dated["YearMonth"] = sales_dated["OrderDate"].dt.to_period("M").astype(str)
    by_month = (
        sales_dated.groupby("YearMonth", as_index=False)
        .agg(TotalSales=("LineTotal","sum"), TotalQty=("OrderQty","sum"))
        .sort_values("YearMonth")
    )
    datasets["sales_by_month"] = by_month
    _save(by_month, "sales_by_month")

    # ── 5. Sales by Year ──────────────────────────────────────
    sales_dated["Year"] = sales_dated["OrderDate"].dt.year
    by_year = (
        sales_dated.groupby("Year", as_index=False)
        .agg(TotalSales=("LineTotal","sum"), TotalQty=("OrderQty","sum"),
             OrderCount=("SalesOrderID","nunique"))
        .sort_values("Year")
    )
    datasets["sales_by_year"] = by_year
    _save(by_year, "sales_by_year")

    # ── 6. Product Count by Color ─────────────────────────────
    color_counts = (
        df_prod.groupby("Color", as_index=False)
        .agg(ProductCount=("ProductID","count"))
        .sort_values("ProductCount", ascending=False)
    )
    datasets["product_by_color"] = color_counts
    _save(color_counts, "product_by_color")

    # ── 7. ListPrice Distribution ─────────────────────────────
    price_df = df_prod[["ProductID","Name","ListPrice","Color"]].copy()
    price_df["ListPrice"] = pd.to_numeric(price_df["ListPrice"], errors="coerce")
    price_df = price_df[price_df["ListPrice"] > 0].dropna(subset=["ListPrice"])
    datasets["list_price_dist"] = price_df
    _save(price_df, "list_price_dist")

    # ── 8. Standard Cost vs List Price ────────────────────────
    cost_price = df_prod[["ProductID","Name","StandardCost","ListPrice","Color"]].copy()
    cost_price["StandardCost"] = pd.to_numeric(cost_price["StandardCost"], errors="coerce")
    cost_price["ListPrice"]    = pd.to_numeric(cost_price["ListPrice"],    errors="coerce")
    cost_price["Margin"]       = cost_price["ListPrice"] - cost_price["StandardCost"]
    cost_price = cost_price[(cost_price["ListPrice"] > 0)].dropna()
    datasets["cost_vs_price"] = cost_price
    _save(cost_price, "cost_vs_price")

    # ── 9. Data Quality Summary ───────────────────────────────
    outlier_prod_path = Path(__file__).resolve().parent.parent / "staging" / "outliers" / "outlier_product.parquet"
    outlier_det_path  = Path(__file__).resolve().parent.parent / "staging" / "outliers" / "outlier_salesorderdetail.parquet"
    quality_rows = [
        {"Category": "Valid Records",     "Count": len(df_prod) + len(df_cust) + len(df_sales)},
        {"Category": "Invalid (Null Key)","Count": 0},
        {"Category": "Duplicates",        "Count": 0},
        {"Category": "Outliers (Product)","Count": len(pd.read_parquet(outlier_prod_path)) if outlier_prod_path.exists() else 0},
        {"Category": "Outliers (Detail)", "Count": len(pd.read_parquet(outlier_det_path))  if outlier_det_path.exists() else 0},
    ]
    dq_df = pd.DataFrame(quality_rows)
    datasets["data_quality_summary"] = dq_df
    _save(dq_df, "data_quality_summary")

    return datasets


# ──────────────────────────────────────────────────────────────
# FULL ANALYTICS PIPELINE
# ──────────────────────────────────────────────────────────────

def run_analytics(
    transformed: dict,
    val_report:  dict,
    olap_data:   dict = None,
) -> dict:
    """
    Build KPIs + all chart datasets and save to dashboard/.

    Parameters
    ----------
    transformed : dict from transformation.transform_all()
    val_report  : report dict from validation.validate_all()["reports"]
    olap_data   : optional dict from olap.build_olap()

    Returns
    -------
    dict with 'kpis' and 'datasets'
    """
    kpis     = compute_kpis(transformed, val_report, olap_data)
    datasets = build_dashboard_datasets(transformed, olap_data)

    # Save KPIs as a single-row DataFrame
    kpi_df = pd.DataFrame([kpis])
    _save(kpi_df, "kpi_summary")

    return {"kpis": kpis, "datasets": datasets}


# ──────────────────────────────────────────────────────────────
# SELF-TEST
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from ingestion      import extract_all
    from validation     import validate_all
    from transformation import transform_all

    print("Running analytics self-test ...\n")
    raw    = extract_all()
    val    = validate_all(raw)
    trn    = transform_all(val["valid"])
    result = run_analytics(trn, val["reports"])

    print("\nKPIs:")
    for k, v in result["kpis"].items():
        if isinstance(v, float):
            print(f"  {k:<25} : {v:>15,.2f}")
        elif isinstance(v, int):
            print(f"  {k:<25} : {v:>15,}")
        else:
            print(f"  {k:<25} : {v}")

    print(f"\nDashboard datasets saved: {len(result['datasets'])}")
    print("Analytics self-test PASSED.")
