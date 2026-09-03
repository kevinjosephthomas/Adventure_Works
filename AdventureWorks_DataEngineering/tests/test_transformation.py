# ============================================================
# tests/test_transformation.py
# AdventureWorks Data Engineering Pipeline
#
# Unit tests for src/transformation.py
# All tests use synthetic DataFrames from conftest.py —
# no CSV files or database connections are needed.
# ============================================================

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

# src/ is added to sys.path by conftest.py
from transformation import (
    transform_product,
    transform_customer,
    transform_sales,
    build_aggregations,
    _trim_strings,
    _to_numeric,
    _to_datetime,
)


# ──────────────────────────────────────────────────────────────
# HELPER FUNCTION TESTS
# ──────────────────────────────────────────────────────────────

class TestHelpers:
    """Tests for private helper functions."""

    def test_trim_strings_removes_whitespace(self):
        """_trim_strings() strips leading/trailing whitespace from string columns."""
        df = pd.DataFrame({"name": ["  Bike  ", "  Helmet"], "num": [1, 2]})
        result = _trim_strings(df)
        assert result["name"].tolist() == ["Bike", "Helmet"]
        assert result["num"].tolist() == [1, 2]  # numeric column unchanged

    def test_to_numeric_coerces_bad_values(self):
        """_to_numeric() converts strings to numbers; bad values become NaN."""
        df = pd.DataFrame({"price": ["99.99", "abc", "0"]})
        result = _to_numeric(df, ["price"])
        assert result["price"].iloc[0] == pytest.approx(99.99)
        assert pd.isna(result["price"].iloc[1])
        assert result["price"].iloc[2] == 0.0

    def test_to_numeric_ignores_missing_columns(self):
        """_to_numeric() silently skips columns that don't exist in the DataFrame."""
        df = pd.DataFrame({"a": [1, 2]})
        # Should not raise even though 'z' doesn't exist
        result = _to_numeric(df, ["a", "z"])
        assert "z" not in result.columns

    def test_to_datetime_parses_dates(self):
        """_to_datetime() converts ISO date strings to datetime dtype."""
        df = pd.DataFrame({"date": ["2013-07-01", "not-a-date", None]})
        result = _to_datetime(df, ["date"])
        assert pd.api.types.is_datetime64_any_dtype(result["date"])
        assert pd.isna(result["date"].iloc[1])   # bad value → NaT
        assert pd.isna(result["date"].iloc[2])   # None → NaT


# ──────────────────────────────────────────────────────────────
# PRODUCT TRANSFORMATION TESTS
# ──────────────────────────────────────────────────────────────

class TestTransformProduct:
    """Tests for transform_product()."""

    @pytest.mark.unit
    def test_drops_null_product_id(self, product_df):
        """Rows with NULL ProductID must be removed."""
        result = transform_product(product_df)
        assert result["ProductID"].notna().all(), "No NaN ProductIDs should remain"

    @pytest.mark.unit
    def test_deduplicates_product_id(self, product_df):
        """Duplicate ProductID rows (keep first) must be removed."""
        result = transform_product(product_df)
        assert result["ProductID"].nunique() == len(result), \
            "ProductID should be unique after deduplication"

    @pytest.mark.unit
    def test_standardizes_color_title_case(self, product_df):
        """Colors should be title-cased (e.g. 'blue' → 'Blue')."""
        result = transform_product(product_df)
        # ProductID 101 had color "blue" → should become "Blue"
        row = result[result["ProductID"] == 101.0]
        assert row["Color"].values[0] == "Blue"

    @pytest.mark.unit
    def test_blank_color_becomes_na(self, product_df):
        """Empty string Color should be replaced with 'N/A'."""
        result = transform_product(product_df)
        # ProductID 102 had blank Color → should become "N/A"
        row = result[result["ProductID"] == 102.0]
        assert row["Color"].values[0] == "N/A"

    @pytest.mark.unit
    def test_invalid_price_flag_negative_list_price(self, product_df):
        """_invalid_price must be True for records with ListPrice < 0."""
        result = transform_product(product_df)
        # ProductID 103 has ListPrice = -5.00
        row = result[result["ProductID"] == 103.0]
        assert row["_invalid_price"].values[0] is True or row["_invalid_price"].values[0] == True

    @pytest.mark.unit
    def test_valid_price_flag_is_false(self, product_df):
        """_invalid_price must be False for products with valid prices."""
        result = transform_product(product_df)
        row = result[result["ProductID"] == 101.0]
        assert not row["_invalid_price"].values[0]

    @pytest.mark.unit
    def test_metadata_column_added(self, product_df):
        """_transformed_at metadata column must be present after transformation."""
        result = transform_product(product_df)
        assert "_transformed_at" in result.columns, \
            "_transformed_at column should be added by transform_product"

    @pytest.mark.unit
    def test_list_price_converted_to_numeric(self, clean_product_df):
        """ListPrice should be a numeric (float) dtype after transformation."""
        result = transform_product(clean_product_df)
        assert pd.api.types.is_float_dtype(result["ListPrice"]) or \
               pd.api.types.is_numeric_dtype(result["ListPrice"])

    @pytest.mark.unit
    def test_date_columns_are_datetime(self, clean_product_df):
        """SellStartDate should be parsed to datetime dtype."""
        result = transform_product(clean_product_df)
        assert pd.api.types.is_datetime64_any_dtype(result["SellStartDate"])

    @pytest.mark.unit
    def test_string_columns_are_stripped(self, product_df):
        """String columns should have no leading/trailing whitespace."""
        result = transform_product(product_df)
        # "Bike  " should become "Bike"
        bike_names = result[result["ProductID"] == 101.0]["Name"].values
        assert len(bike_names) > 0
        assert bike_names[0] == "Bike"


# ──────────────────────────────────────────────────────────────
# CUSTOMER TRANSFORMATION TESTS
# ──────────────────────────────────────────────────────────────

class TestTransformCustomer:
    """Tests for transform_customer()."""

    @pytest.mark.unit
    def test_drops_null_customer_id(self, customer_df):
        """Rows with NULL CustomerID must be removed."""
        result = transform_customer(customer_df)
        assert result["CustomerID"].notna().all()

    @pytest.mark.unit
    def test_deduplicates_customer_id(self, customer_df):
        """Duplicate CustomerID rows must be reduced to one."""
        result = transform_customer(customer_df)
        assert result["CustomerID"].nunique() == len(result)

    @pytest.mark.unit
    def test_fills_person_id_with_minus_one(self, customer_df):
        """NULL PersonID should be filled with -1."""
        result = transform_customer(customer_df)
        assert (result["PersonID"] == -1).sum() >= 1, \
            "At least one NULL PersonID should have been filled with -1"
        assert result["PersonID"].isna().sum() == 0

    @pytest.mark.unit
    def test_fills_store_id_with_minus_one(self, customer_df):
        """NULL StoreID should be filled with -1."""
        result = transform_customer(customer_df)
        assert result["StoreID"].isna().sum() == 0

    @pytest.mark.unit
    def test_account_number_uppercased(self, customer_df):
        """AccountNumber should be converted to uppercase."""
        result = transform_customer(customer_df)
        for val in result["AccountNumber"].dropna():
            assert val == val.upper(), f"AccountNumber '{val}' is not uppercase"

    @pytest.mark.unit
    def test_metadata_column_added(self, customer_df):
        """_transformed_at must be added."""
        result = transform_customer(customer_df)
        assert "_transformed_at" in result.columns


# ──────────────────────────────────────────────────────────────
# SALES TRANSFORMATION TESTS
# ──────────────────────────────────────────────────────────────

class TestTransformSales:
    """Tests for transform_sales() — join Header + Detail."""

    @pytest.mark.unit
    def test_inner_join_produces_correct_row_count(self, sales_header_df, sales_detail_df):
        """
        Inner join on SalesOrderID should produce at most len(detail) rows.
        Since detail has SalesOrderIDs 1001, 1002, 1003 (all in header),
        result should have 5 rows minus the NULL ProductID row = 4 rows.
        """
        result = transform_sales(sales_header_df, sales_detail_df)
        # After dropping rows with NULL ProductID (SalesOrderDetailID=4),
        # we expect 4 valid rows
        assert len(result) == 4, f"Expected 4 rows after null-key drop, got {len(result)}"

    @pytest.mark.unit
    def test_null_product_id_rows_dropped_after_join(self, sales_header_df, sales_detail_df):
        """Rows with NULL ProductID must be dropped from the joined result."""
        result = transform_sales(sales_header_df, sales_detail_df)
        assert result["ProductID"].notna().all()

    @pytest.mark.unit
    def test_join_brings_customer_id_from_header(self, sales_header_df, sales_detail_df):
        """CustomerID should come from header and be present in joined result."""
        result = transform_sales(sales_header_df, sales_detail_df)
        assert "CustomerID" in result.columns
        assert result["CustomerID"].notna().all()

    @pytest.mark.unit
    def test_metadata_column_added(self, sales_header_df, sales_detail_df):
        """_transformed_at must be added to joined sales data."""
        result = transform_sales(sales_header_df, sales_detail_df)
        assert "_transformed_at" in result.columns

    @pytest.mark.unit
    def test_order_date_is_datetime(self, sales_header_df, sales_detail_df):
        """OrderDate should be parsed to datetime."""
        result = transform_sales(sales_header_df, sales_detail_df)
        assert pd.api.types.is_datetime64_any_dtype(result["OrderDate"])


# ──────────────────────────────────────────────────────────────
# AGGREGATION TESTS
# ──────────────────────────────────────────────────────────────

class TestBuildAggregations:
    """Tests for build_aggregations()."""

    @pytest.mark.unit
    def test_aggregations_returns_expected_keys(self, clean_sales_df, clean_product_df, customer_df):
        """build_aggregations() must return a dict with all 7 expected keys."""
        # Transform customer_df first to get a valid customer df
        from transformation import transform_customer
        clean_customer = transform_customer(customer_df)

        result = build_aggregations(clean_sales_df, clean_product_df, clean_customer)
        expected_keys = {
            "sales_by_product", "sales_by_customer", "avg_product_price",
            "top10_products", "sales_by_date", "sales_by_month", "sales_by_year",
        }
        assert expected_keys == set(result.keys())

    @pytest.mark.unit
    def test_sales_by_product_sorted_descending(self, clean_sales_df, clean_product_df, customer_df):
        """sales_by_product should be sorted by TotalSales descending."""
        from transformation import transform_customer
        clean_customer = transform_customer(customer_df)

        result = build_aggregations(clean_sales_df, clean_product_df, clean_customer)
        total_sales = result["sales_by_product"]["TotalSales"].tolist()
        assert total_sales == sorted(total_sales, reverse=True), \
            "sales_by_product must be sorted by TotalSales descending"

    @pytest.mark.unit
    def test_top10_products_is_subset_of_by_product(self, clean_sales_df, clean_product_df, customer_df):
        """top10_products must have at most 10 rows and be a subset of sales_by_product."""
        from transformation import transform_customer
        clean_customer = transform_customer(customer_df)

        result = build_aggregations(clean_sales_df, clean_product_df, clean_customer)
        assert len(result["top10_products"]) <= 10

    @pytest.mark.unit
    def test_sales_by_date_has_date_and_total_sales_cols(self, clean_sales_df, clean_product_df, customer_df):
        """sales_by_date must contain 'Date' and 'TotalSales' columns."""
        from transformation import transform_customer
        clean_customer = transform_customer(customer_df)

        result = build_aggregations(clean_sales_df, clean_product_df, clean_customer)
        assert "Date" in result["sales_by_date"].columns
        assert "TotalSales" in result["sales_by_date"].columns

    @pytest.mark.unit
    def test_sales_by_year_aggregates_correctly(self, clean_sales_df, clean_product_df, customer_df):
        """sales_by_year should have one row per year and correct total."""
        from transformation import transform_customer
        clean_customer = transform_customer(customer_df)

        result = build_aggregations(clean_sales_df, clean_product_df, clean_customer)
        # All 3 rows are in 2013 — should have exactly 1 year row
        by_year = result["sales_by_year"]
        assert len(by_year) == 1
        assert 2013 in by_year["Year"].values
        expected_total = clean_sales_df["LineTotal"].sum()
        assert by_year["TotalSales"].values[0] == pytest.approx(expected_total)
