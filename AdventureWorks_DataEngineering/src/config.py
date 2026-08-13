# ============================================================
# AdventureWorks Data Engineering Pipeline
# config.py — Central Configuration File
#
# HOW TO USE:
#   Change DATA_PATH below to point to your source CSV files.
#   All other modules import from this file — never hard-code
#   paths anywhere else in the project.
# ============================================================

import os
from pathlib import Path

# ──────────────────────────────────────────────────────────────
# ROOT PATHS
# ──────────────────────────────────────────────────────────────

# Project root: the folder containing /src, /data, /notebooks …
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ──────────────────────────────────────────────────────────────
# DATA SOURCE CONFIGURATION  ← CHANGE THIS PATH TO YOUR CSVs
# ──────────────────────────────────────────────────────────────

# DATA_PATH points to the folder that contains:
#   Product.csv, Customer.csv, SalesOrderHeader.csv, SalesOrderDetail.csv
#
# Options (uncomment the one you want):

# Option 1: Use the bundled data/ folder inside the project
DATA_PATH = PROJECT_ROOT / "data" / "AdventureWorks"

# Option 2: Point to any folder on your machine
# DATA_PATH = Path(r"C:\MyData\AdventureWorks")

# Option 3: Use an environment variable (e.g. set AW_DATA_PATH=C:\... in your shell)
# DATA_PATH = Path(os.environ.get("AW_DATA_PATH", str(PROJECT_ROOT / "data" / "AdventureWorks")))

# ──────────────────────────────────────────────────────────────
# SOURCE FILE NAMES (do not change unless your files differ)
# ──────────────────────────────────────────────────────────────

SOURCE_FILES = {
    "product":            DATA_PATH / "Product.csv",
    "customer":           DATA_PATH / "Customer.csv",
    "salesorderheader":   DATA_PATH / "SalesOrderHeader.csv",
    "salesorderdetail":   DATA_PATH / "SalesOrderDetail.csv",
}

# ──────────────────────────────────────────────────────────────
# CSV FORMAT
# The AdventureWorks exports are tab-separated with NO header row.
# ──────────────────────────────────────────────────────────────

CSV_SEPARATOR   = "\t"
CSV_HAS_HEADER  = False   # Files have no header row — column names defined below

# ──────────────────────────────────────────────────────────────
# COLUMN DEFINITIONS
# Define the column names that map positionally to each CSV file.
# ──────────────────────────────────────────────────────────────

PRODUCT_COLUMNS = [
    "ProductID", "Name", "ProductNumber", "MakeFlag", "FinishedGoodsFlag",
    "Color", "SafetyStockLevel", "ReorderPoint", "StandardCost", "ListPrice",
    "Size", "SizeUnitMeasureCode", "WeightUnitMeasureCode", "Weight",
    "DaysToManufacture", "ProductLine", "Class", "Style",
    "ProductSubcategoryID", "ProductModelID",
    "SellStartDate", "SellEndDate", "DiscontinuedDate",
    "rowguid", "ModifiedDate"
]

CUSTOMER_COLUMNS = [
    "CustomerID", "PersonID", "StoreID", "TerritoryID",
    "AccountNumber", "rowguid", "ModifiedDate"
]

SALESORDERHEADER_COLUMNS = [
    "SalesOrderID", "RevisionNumber", "OrderDate", "DueDate", "ShipDate",
    "Status", "OnlineOrderFlag", "SalesOrderNumber", "PurchaseOrderNumber",
    "AccountNumber", "CustomerID", "SalesPersonID", "TerritoryID",
    "BillToAddressID", "ShipToAddressID", "ShipMethodID", "CreditCardID",
    "CreditCardApprovalCode", "CurrencyRateID", "SubTotal", "TaxAmt",
    "Freight", "TotalDue", "Comment", "rowguid", "ModifiedDate"
]

SALESORDERDETAIL_COLUMNS = [
    "SalesOrderID", "SalesOrderDetailID", "CarrierTrackingNumber",
    "OrderQty", "ProductID", "SpecialOfferID", "UnitPrice",
    "UnitPriceDiscount", "LineTotal", "rowguid", "ModifiedDate"
]

# ──────────────────────────────────────────────────────────────
# STAGING PATHS
# ──────────────────────────────────────────────────────────────

STAGING_ROOT        = PROJECT_ROOT / "staging"
STAGING_RAW         = STAGING_ROOT / "raw"
STAGING_VALID       = STAGING_ROOT / "valid"
STAGING_INVALID     = STAGING_ROOT / "invalid"
STAGING_DUPLICATES  = STAGING_ROOT / "duplicates"
STAGING_OUTLIERS    = STAGING_ROOT / "outliers"

# ──────────────────────────────────────────────────────────────
# LOGS
# ──────────────────────────────────────────────────────────────

LOGS_DIR            = PROJECT_ROOT / "logs"
PIPELINE_LOG_FILE   = LOGS_DIR / "pipeline.log"

# ──────────────────────────────────────────────────────────────
# DASHBOARD OUTPUT
# ──────────────────────────────────────────────────────────────

DASHBOARD_DIR       = PROJECT_ROOT / "dashboard"

# ──────────────────────────────────────────────────────────────
# DATABASE CONFIGURATION  ← CHANGE TO YOUR PostgreSQL SETTINGS
# ──────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host":     os.environ.get("AW_DB_HOST",     "localhost"),
    "port":     int(os.environ.get("AW_DB_PORT", "5432")),
    "database": os.environ.get("AW_DB_NAME",     "adventureworks_de"),
    "user":     os.environ.get("AW_DB_USER",     "postgres"),
    "password": os.environ.get("AW_DB_PASSWORD", "postgres"),
}

# SQLAlchemy connection string (built from DB_CONFIG above)
DB_CONNECTION_STRING = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# ──────────────────────────────────────────────────────────────
# PYSPARK CONFIGURATION
# ──────────────────────────────────────────────────────────────

SPARK_APP_NAME      = "AdventureWorks_DE_Pipeline"
SPARK_MASTER        = "local[*]"         # Use all available CPU cores locally
SPARK_DRIVER_MEMORY = "4g"

# ──────────────────────────────────────────────────────────────
# PIPELINE SETTINGS
# ──────────────────────────────────────────────────────────────

PIPELINE_MODE       = "FULL_REFRESH"     # Always full refresh in main pipeline

# IQR multiplier for outlier detection (used in Exercise 4)
OUTLIER_IQR_MULTIPLIER = 1.5

# ──────────────────────────────────────────────────────────────
# HELPER: ensure all output directories exist
# ──────────────────────────────────────────────────────────────

def ensure_directories():
    """Create all required output directories if they do not exist."""
    dirs = [
        STAGING_RAW, STAGING_VALID, STAGING_INVALID,
        STAGING_DUPLICATES, STAGING_OUTLIERS,
        LOGS_DIR, DASHBOARD_DIR,
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


# ──────────────────────────────────────────────────────────────
# QUICK SELF-TEST
# Run: python config.py  to verify your paths are correct.
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("AdventureWorks DE Pipeline - Configuration Check")
    print("=" * 60)
    print(f"\nProject Root : {PROJECT_ROOT}")
    print(f"Data Path    : {DATA_PATH}")
    print()
    print("Source Files:")
    for name, path in SOURCE_FILES.items():
        status = "[FOUND]  " if path.exists() else "[MISSING]"
        print(f"  {status}  {name:20s}  ->  {path}")
    print()
    print("Database:")
    print(f"  Connection  : {DB_CONNECTION_STRING}")
    print()
    ensure_directories()
    print("Staging & log directories: OK")
    print("=" * 60)
