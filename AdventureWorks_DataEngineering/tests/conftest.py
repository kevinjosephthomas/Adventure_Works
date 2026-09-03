# ============================================================
# tests/conftest.py
# AdventureWorks Data Engineering Pipeline — Shared Test Fixtures
#
# All fixtures return synthetic pandas DataFrames that mirror
# the real schema.  No CSV files or database are required.
# ============================================================

import sys
import pytest
import pandas as pd
import numpy as np
from pathlib import Path

# ── Make src/ importable without installing the package ──────
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))


# ──────────────────────────────────────────────────────────────
# PRODUCT FIXTURES
# ──────────────────────────────────────────────────────────────

@pytest.fixture
def product_df():
    """
    Minimal synthetic Product DataFrame mirroring the real schema.
    Contains:
      - 1 normal product (valid, Blue, positive price)
      - 1 product with NULL ProductID    → should be dropped
      - 1 duplicate ProductID (101)      → second occurrence should be dropped
      - 1 product with blank Color       → should become 'N/A'
      - 1 product with negative ListPrice → _invalid_price should be True
    """
    return pd.DataFrame({
        "ProductID":          [101,      None,    101,     102,      103],
        "Name":               ["Bike  ", "Helmet", "Bike  ", "Gloves", "Chain"],
        "ProductNumber":      ["BK-001", "HE-001", "BK-001", "GL-001", "CH-001"],
        "MakeFlag":           [1,        1,        1,        0,        1],
        "FinishedGoodsFlag":  [1,        1,        1,        1,        0],
        "Color":              ["blue",   "Red",    "blue",   "",       "black"],
        "SafetyStockLevel":   [100,      50,       100,      200,      75],
        "ReorderPoint":       [50,       25,       50,       100,      30],
        "StandardCost":       [500.00,   75.00,    500.00,   20.00,    10.00],
        "ListPrice":          [999.99,   149.99,   999.99,   49.99,    -5.00],
        "Weight":             [12.5,     0.8,      12.5,     0.2,      0.1],
        "DaysToManufacture":  [4,        2,        4,        1,        1],
        "ProductSubcategoryID": [1,      2,        1,        3,        4],
        "ProductModelID":     [10,       20,       10,       30,       40],
        "SellStartDate":      ["2011-07-01"] * 5,
        "SellEndDate":        [None] * 5,
        "DiscontinuedDate":   [None] * 5,
        "ModifiedDate":       ["2014-02-08"] * 5,
    })


@pytest.fixture
def clean_product_df():
    """A fully valid product DataFrame — no nulls, no dupes, no bad prices."""
    return pd.DataFrame({
        "ProductID":          [201, 202, 203],
        "Name":               ["Road Bike", "Mountain Bike", "Helmet"],
        "ProductNumber":      ["RB-001", "MB-001", "HE-001"],
        "MakeFlag":           [1, 1, 0],
        "FinishedGoodsFlag":  [1, 1, 1],
        "Color":              ["Red", "Black", "Blue"],
        "SafetyStockLevel":   [100, 150, 200],
        "ReorderPoint":       [50, 75, 100],
        "StandardCost":       [300.00, 400.00, 50.00],
        "ListPrice":          [999.00, 1299.00, 149.00],
        "Weight":             [10.0, 14.0, 0.8],
        "DaysToManufacture":  [4, 5, 2],
        "ProductSubcategoryID": [1, 1, 2],
        "ProductModelID":     [10, 11, 20],
        "SellStartDate":      ["2011-07-01"] * 3,
        "SellEndDate":        [None] * 3,
        "DiscontinuedDate":   [None] * 3,
        "ModifiedDate":       ["2014-02-08"] * 3,
    })


# ──────────────────────────────────────────────────────────────
# CUSTOMER FIXTURES
# ──────────────────────────────────────────────────────────────

@pytest.fixture
def customer_df():
    """
    Synthetic Customer DataFrame.
    Contains:
      - 1 normal customer
      - 1 customer with NULL CustomerID → should be dropped
      - 1 duplicate CustomerID (301)   → second should be dropped
      - 1 customer with null PersonID  → should become -1
      - 1 with lowercase AccountNumber → should become uppercase
    """
    return pd.DataFrame({
        "CustomerID":    [301,      None,   301,     302,      303],
        "PersonID":      [1001,     None,   1001,    None,     1003],
        "StoreID":       [None,     2001,   None,    2002,     None],
        "TerritoryID":   [1,        2,      1,       3,        4],
        "AccountNumber": ["aw00001", "aw00002", "aw00001", "AW00003", "aw00004"],
        "ModifiedDate":  ["2014-09-01"] * 5,
    })


# ──────────────────────────────────────────────────────────────
# SALES FIXTURES
# ──────────────────────────────────────────────────────────────

@pytest.fixture
def sales_header_df():
    """
    Synthetic SalesOrderHeader DataFrame — 3 unique orders.
    """
    return pd.DataFrame({
        "SalesOrderID":    [1001, 1002, 1003],
        "RevisionNumber":  [1, 1, 2],
        "OrderDate":       ["2013-07-01", "2013-07-02", "2013-07-03"],
        "DueDate":         ["2013-07-13", "2013-07-14", "2013-07-15"],
        "ShipDate":        ["2013-07-08", "2013-07-09", "2013-07-10"],
        "Status":          [5, 5, 5],
        "OnlineOrderFlag": [1, 0, 1],
        "CustomerID":      [301, 302, 303],
        "TerritoryID":     [1, 3, 4],
        "SubTotal":        [1000.00, 500.00, 250.00],
        "TaxAmt":          [80.00, 40.00, 20.00],
        "Freight":         [25.00, 12.50, 6.25],
        "TotalDue":        [1105.00, 552.50, 276.25],
        "ModifiedDate":    ["2013-07-08"] * 3,
    })


@pytest.fixture
def sales_detail_df():
    """
    Synthetic SalesOrderDetail DataFrame — 5 line items across 3 orders.
    Includes one row with NULL ProductID which should be dropped after join.
    """
    return pd.DataFrame({
        "SalesOrderID":       [1001, 1001, 1002, 1003, 1003],
        "SalesOrderDetailID": [1,    2,    3,    4,    5],
        "CarrierTrackingNum": ["4911-403C-98", "4911-403C-98", "6431-4D57-83", "6431-4D57-84", "6431-4D57-85"],
        "OrderQty":           [2,    1,    3,    1,    2],
        "ProductID":          [201,  202,  201,  None, 203],
        "UnitPrice":          [999.00, 1299.00, 999.00, 149.00, 999.00],
        "UnitPriceDiscount":  [0.0, 0.0, 0.0, 0.0, 0.0],
        "LineTotal":          [1998.00, 1299.00, 2997.00, 149.00, 1998.00],
        "ModifiedDate":       ["2013-07-08"] * 5,
    })


# ──────────────────────────────────────────────────────────────
# COMBINED CLEAN SALES — for aggregation tests
# ──────────────────────────────────────────────────────────────

@pytest.fixture
def clean_sales_df():
    """
    A clean, pre-joined sales DataFrame ready for aggregation tests.
    """
    return pd.DataFrame({
        "SalesOrderID":       [1001, 1001, 1002],
        "SalesOrderDetailID": [1,    2,    3],
        "ProductID":          [201,  202,  201],
        "CustomerID":         [301,  301,  302],
        "OrderDate":          pd.to_datetime(["2013-07-01", "2013-07-01", "2013-07-02"]),
        "DueDate":            pd.to_datetime(["2013-07-13", "2013-07-13", "2013-07-14"]),
        "ShipDate":           pd.to_datetime(["2013-07-08", "2013-07-08", "2013-07-09"]),
        "OrderQty":           [2,    1,    3],
        "UnitPrice":          [999.00, 1299.00, 999.00],
        "UnitPriceDiscount":  [0.0, 0.0, 0.0],
        "LineTotal":          [1998.00, 1299.00, 2997.00],
        "SubTotal":           [1000.00, 1000.00, 500.00],
        "TaxAmt":             [80.00, 80.00, 40.00],
        "Freight":            [25.00, 25.00, 12.50],
        "TotalDue":           [1105.00, 1105.00, 552.50],
        "OnlineOrderFlag":    [1, 1, 0],
        "TerritoryID":        [1, 1, 3],
    })
