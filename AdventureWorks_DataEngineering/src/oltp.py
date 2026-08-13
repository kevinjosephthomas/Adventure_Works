# ============================================================
# src/oltp.py
# AdventureWorks Data Engineering Pipeline — Step 2
#
# Responsibility:
#   - Create the OLTP schema in PostgreSQL
#   - Load transformed data into OLTP tables (full refresh)
#   - Use SQLAlchemy + transactions for atomicity
#   - Handle connection errors gracefully
# ============================================================

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DB_CONNECTION_STRING, STAGING_VALID

# ── SQLAlchemy (optional import — graceful fail if not installed) ──
try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import SQLAlchemyError
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False


# ──────────────────────────────────────────────────────────────
# CONNECTION
# ──────────────────────────────────────────────────────────────

def get_engine(connection_string: str = DB_CONNECTION_STRING):
    """
    Create and return a SQLAlchemy engine.
    Returns None if SQLAlchemy is not available or connection fails.
    """
    if not SQLALCHEMY_AVAILABLE:
        print("    [OLTP] SQLAlchemy not installed — skipping DB load.")
        return None
    try:
        engine = create_engine(connection_string, pool_pre_ping=True)
        # Test the connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"    [OLTP] Connected to database.")
        return engine
    except Exception as e:
        print(f"    [OLTP] Cannot connect to database: {e}")
        print("    [OLTP] Skipping DB operations — data saved in staging/valid/ only.")
        return None


# ──────────────────────────────────────────────────────────────
# SCHEMA CREATION
# ──────────────────────────────────────────────────────────────

def create_oltp_schema(engine) -> bool:
    """
    Execute the OLTP DDL to create tables in PostgreSQL.
    Reads sql/oltp_schema.sql from the project.

    Full Refresh: tables are DROPped and re-CREATEd every run.
    """
    if engine is None:
        return False

    sql_path = Path(__file__).resolve().parent.parent / "sql" / "oltp_schema.sql"
    if not sql_path.exists():
        print(f"    [OLTP] SQL file not found: {sql_path}")
        return False

    ddl = sql_path.read_text(encoding="utf-8")

    try:
        with engine.begin() as conn:
            # Execute each statement separated by semicolons
            statements = [s.strip() for s in ddl.split(";") if s.strip()]
            for stmt in statements:
                conn.execute(text(stmt))
        print("    [OLTP] Schema created (oltp.product, customer, salesorderheader, salesorderdetail)")
        return True
    except Exception as e:
        print(f"    [OLTP] Schema creation failed: {e}")
        return False


# ──────────────────────────────────────────────────────────────
# DATA LOADERS  (each table is loaded in a single transaction)
# ──────────────────────────────────────────────────────────────

def _rename_for_oltp_product(df: pd.DataFrame) -> pd.DataFrame:
    """Rename Python-style DataFrame columns to match OLTP DDL column names."""
    rename_map = {
        "ProductID":              "product_id",
        "Name":                   "name",
        "ProductNumber":          "product_number",
        "MakeFlag":               "make_flag",
        "FinishedGoodsFlag":      "finished_goods_flag",
        "Color":                  "color",
        "SafetyStockLevel":       "safety_stock_level",
        "ReorderPoint":           "reorder_point",
        "StandardCost":           "standard_cost",
        "ListPrice":              "list_price",
        "Size":                   "size",
        "SizeUnitMeasureCode":    "size_unit_measure_code",
        "WeightUnitMeasureCode":  "weight_unit_measure_code",
        "Weight":                 "weight",
        "DaysToManufacture":      "days_to_manufacture",
        "ProductLine":            "product_line",
        "Class":                  "class",
        "Style":                  "style",
        "ProductSubcategoryID":   "product_subcategory_id",
        "ProductModelID":         "product_model_id",
        "SellStartDate":          "sell_start_date",
        "SellEndDate":            "sell_end_date",
        "DiscontinuedDate":       "discontinued_date",
        "rowguid":                "rowguid",
        "ModifiedDate":           "modified_date",
        "_invalid_price":         "is_invalid_price",
        "_is_outlier":            "is_outlier",
    }
    cols_available = {k: v for k, v in rename_map.items() if k in df.columns}
    return df.rename(columns=cols_available)


def _rename_for_oltp_customer(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "CustomerID":    "customer_id",
        "PersonID":      "person_id",
        "StoreID":       "store_id",
        "TerritoryID":   "territory_id",
        "AccountNumber": "account_number",
        "rowguid":       "rowguid",
        "ModifiedDate":  "modified_date",
    }
    cols_available = {k: v for k, v in rename_map.items() if k in df.columns}
    return df.rename(columns=cols_available)


def _rename_for_oltp_header(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "SalesOrderID":           "sales_order_id",
        "RevisionNumber":         "revision_number",
        "OrderDate":              "order_date",
        "DueDate":                "due_date",
        "ShipDate":               "ship_date",
        "Status":                 "status",
        "OnlineOrderFlag":        "online_order_flag",
        "SalesOrderNumber":       "sales_order_number",
        "PurchaseOrderNumber":    "purchase_order_number",
        "AccountNumber":          "account_number",
        "CustomerID":             "customer_id",
        "SalesPersonID":          "sales_person_id",
        "TerritoryID":            "territory_id",
        "BillToAddressID":        "bill_to_address_id",
        "ShipToAddressID":        "ship_to_address_id",
        "ShipMethodID":           "ship_method_id",
        "CreditCardID":           "credit_card_id",
        "CreditCardApprovalCode": "credit_card_approval_code",
        "CurrencyRateID":         "currency_rate_id",
        "SubTotal":               "sub_total",
        "TaxAmt":                 "tax_amt",
        "Freight":                "freight",
        "TotalDue":               "total_due",
        "Comment":                "comment",
        "rowguid":                "rowguid",
        "ModifiedDate":           "modified_date",
    }
    cols_available = {k: v for k, v in rename_map.items() if k in df.columns}
    return df.rename(columns=cols_available)


def _rename_for_oltp_detail(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "SalesOrderID":         "sales_order_id",
        "SalesOrderDetailID":   "sales_order_detail_id",
        "CarrierTrackingNumber":"carrier_tracking_number",
        "OrderQty":             "order_qty",
        "ProductID":            "product_id",
        "SpecialOfferID":       "special_offer_id",
        "UnitPrice":            "unit_price",
        "UnitPriceDiscount":    "unit_price_discount",
        "LineTotal":            "line_total",
        "rowguid":              "rowguid",
        "ModifiedDate":         "modified_date",
    }
    cols_available = {k: v for k, v in rename_map.items() if k in df.columns}
    return df.rename(columns=cols_available)


def _keep_oltp_cols(df: pd.DataFrame, target_cols: list) -> pd.DataFrame:
    """Keep only columns that exist in both DataFrame and target list."""
    keep = [c for c in target_cols if c in df.columns]
    return df[keep].copy()


def load_oltp(engine, transformed: dict) -> dict:
    """
    Full-refresh load of all OLTP tables.

    Order: product -> customer -> salesorderheader -> salesorderdetail
    (respects foreign key dependencies)

    Parameters
    ----------
    engine      : SQLAlchemy engine (from get_engine())
    transformed : dict from transformation.transform_all()

    Returns
    -------
    dict with row counts per table
    """
    if engine is None:
        print("    [OLTP] No database connection — skipping load.")
        return {}

    counts = {}

    # ── Load Product ──────────────────────────────────────────
    try:
        df_prod = _rename_for_oltp_product(transformed["product"].copy())
        # Drop internal metadata columns
        drop_cols = [c for c in df_prod.columns if c.startswith("_")]
        df_prod   = df_prod.drop(columns=drop_cols, errors="ignore")

        prod_cols = [
            "product_id", "name", "product_number", "make_flag", "finished_goods_flag",
            "color", "safety_stock_level", "reorder_point", "standard_cost", "list_price",
            "size", "weight", "days_to_manufacture", "product_line", "class", "style",
            "product_subcategory_id", "product_model_id",
            "sell_start_date", "sell_end_date", "discontinued_date",
            "rowguid", "modified_date", "is_invalid_price", "is_outlier",
        ]
        df_prod = _keep_oltp_cols(df_prod, prod_cols)

        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE oltp.product CASCADE"))
            df_prod.to_sql("product", conn, schema="oltp", if_exists="append",
                           index=False, method="multi", chunksize=500)
        counts["product"] = len(df_prod)
        print(f"    [OLTP] product        : {len(df_prod):,} rows loaded")
    except Exception as e:
        print(f"    [OLTP] product load failed: {e}")

    # ── Load Customer ─────────────────────────────────────────
    try:
        df_cust = _rename_for_oltp_customer(transformed["customer"].copy())
        drop_cols = [c for c in df_cust.columns if c.startswith("_")]
        df_cust   = df_cust.drop(columns=drop_cols, errors="ignore")

        cust_cols = ["customer_id", "person_id", "store_id", "territory_id",
                     "account_number", "rowguid", "modified_date"]
        df_cust = _keep_oltp_cols(df_cust, cust_cols)

        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE oltp.customer CASCADE"))
            df_cust.to_sql("customer", conn, schema="oltp", if_exists="append",
                           index=False, method="multi", chunksize=500)
        counts["customer"] = len(df_cust)
        print(f"    [OLTP] customer       : {len(df_cust):,} rows loaded")
    except Exception as e:
        print(f"    [OLTP] customer load failed: {e}")

    # ── Load SalesOrderHeader ─────────────────────────────────
    try:
        # Use the raw valid header (before joining with detail)
        hdr_path = STAGING_VALID / "valid_salesorderheader.parquet"
        if hdr_path.exists():
            df_hdr = pd.read_parquet(hdr_path)
        else:
            df_hdr = transformed.get("salesorderheader", pd.DataFrame())

        df_hdr = _rename_for_oltp_header(df_hdr.copy())
        drop_cols = [c for c in df_hdr.columns if c.startswith("_")]
        df_hdr    = df_hdr.drop(columns=drop_cols, errors="ignore")

        hdr_cols = [
            "sales_order_id", "revision_number", "order_date", "due_date", "ship_date",
            "status", "online_order_flag", "sales_order_number", "purchase_order_number",
            "account_number", "customer_id", "sales_person_id", "territory_id",
            "bill_to_address_id", "ship_to_address_id", "ship_method_id",
            "sub_total", "tax_amt", "freight", "total_due", "rowguid", "modified_date",
        ]
        df_hdr = _keep_oltp_cols(df_hdr, hdr_cols)
        # Only load headers whose customer_id exists in customer table
        valid_cust_ids = set(transformed["customer"]["CustomerID"].dropna().astype(int))
        if "customer_id" in df_hdr.columns:
            df_hdr["customer_id"] = pd.to_numeric(df_hdr["customer_id"], errors="coerce")
            df_hdr = df_hdr[df_hdr["customer_id"].isin(valid_cust_ids)]

        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE oltp.salesorderheader CASCADE"))
            df_hdr.to_sql("salesorderheader", conn, schema="oltp", if_exists="append",
                          index=False, method="multi", chunksize=500)
        counts["salesorderheader"] = len(df_hdr)
        print(f"    [OLTP] salesorderheader : {len(df_hdr):,} rows loaded")
    except Exception as e:
        print(f"    [OLTP] salesorderheader load failed: {e}")

    # ── Load SalesOrderDetail ─────────────────────────────────
    try:
        dtl_path = STAGING_VALID / "valid_salesorderdetail.parquet"
        if dtl_path.exists():
            df_dtl = pd.read_parquet(dtl_path)
        else:
            df_dtl = transformed.get("salesorderdetail", pd.DataFrame())

        df_dtl = _rename_for_oltp_detail(df_dtl.copy())
        drop_cols = [c for c in df_dtl.columns if c.startswith("_")]
        df_dtl    = df_dtl.drop(columns=drop_cols, errors="ignore")

        dtl_cols = [
            "sales_order_id", "sales_order_detail_id", "carrier_tracking_number",
            "order_qty", "product_id", "special_offer_id",
            "unit_price", "unit_price_discount", "line_total",
            "rowguid", "modified_date",
        ]
        df_dtl = _keep_oltp_cols(df_dtl, dtl_cols)

        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE oltp.salesorderdetail CASCADE"))
            df_dtl.to_sql("salesorderdetail", conn, schema="oltp", if_exists="append",
                          index=False, method="multi", chunksize=1000)
        counts["salesorderdetail"] = len(df_dtl)
        print(f"    [OLTP] salesorderdetail : {len(df_dtl):,} rows loaded")
    except Exception as e:
        print(f"    [OLTP] salesorderdetail load failed: {e}")

    return counts


# ──────────────────────────────────────────────────────────────
# SELF-TEST
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("OLTP self-test — testing connection only (no data load)")
    engine = get_engine()
    if engine:
        print("Connection OK.")
    else:
        print("No DB connection (expected if PostgreSQL not running).")
