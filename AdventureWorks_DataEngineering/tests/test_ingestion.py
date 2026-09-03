# ============================================================
# tests/test_ingestion.py
# AdventureWorks Data Engineering Pipeline
#
# Unit tests for src/ingestion.py
# Uses unittest.mock to avoid reading real CSV files.
# ============================================================

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys

# src/ is on sys.path from conftest.py
import ingestion
from ingestion import (
    extract_product,
    extract_customer,
    extract_all,
    stage_raw,
)


# ──────────────────────────────────────────────────────────────
# _read_csv TESTS
# ──────────────────────────────────────────────────────────────

class TestReadCsv:
    """Tests for the internal _read_csv helper."""

    @pytest.mark.unit
    def test_raises_file_not_found_for_missing_path(self):
        """
        _read_csv() must raise FileNotFoundError when the source CSV
        does not exist at the configured path.
        """
        # Override SOURCE_FILES to point at a path that definitely doesn't exist
        fake_path = Path("Z:/this_path/does_not_exist.csv")
        with patch.dict(ingestion.SOURCE_FILES, {"product": fake_path}):
            with pytest.raises(FileNotFoundError, match="Source file NOT FOUND"):
                ingestion._read_csv("product", ["col1", "col2"])

    @pytest.mark.unit
    def test_metadata_columns_added_after_read(self, tmp_path):
        """
        After a successful read, the DataFrame must have
        _source_file and _ingested_at metadata columns.
        """
        # Create a tiny real temp CSV so we can test the metadata addition
        csv_content = "101\tBike\n102\tHelmet\n"
        csv_file = tmp_path / "test_product.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        with patch.dict(ingestion.SOURCE_FILES, {"product": csv_file}):
            with patch.object(ingestion, "CSV_SEPARATOR", "\t"):
                df = ingestion._read_csv("product", ["ProductID", "Name"])

        assert "_source_file" in df.columns, "_source_file metadata column missing"
        assert "_ingested_at" in df.columns, "_ingested_at metadata column missing"

    @pytest.mark.unit
    def test_metadata_source_file_is_string(self, tmp_path):
        """_source_file column should contain the string path of the source file."""
        csv_file = tmp_path / "product.csv"
        csv_file.write_text("1\tBike\n", encoding="utf-8")

        with patch.dict(ingestion.SOURCE_FILES, {"product": csv_file}):
            with patch.object(ingestion, "CSV_SEPARATOR", "\t"):
                df = ingestion._read_csv("product", ["ProductID", "Name"])

        assert str(csv_file) in df["_source_file"].iloc[0]

    @pytest.mark.unit
    def test_correct_columns_assigned(self, tmp_path):
        """Columns provided to _read_csv() must be assigned positionally."""
        csv_file = tmp_path / "product.csv"
        csv_file.write_text("101\tBike\t999.99\n", encoding="utf-8")

        col_names = ["ProductID", "Name", "ListPrice"]
        with patch.dict(ingestion.SOURCE_FILES, {"product": csv_file}):
            with patch.object(ingestion, "CSV_SEPARATOR", "\t"):
                df = ingestion._read_csv("product", col_names)

        assert list(df.columns[:3]) == col_names


# ──────────────────────────────────────────────────────────────
# extract_all TESTS
# ──────────────────────────────────────────────────────────────

class TestExtractAll:
    """Tests for extract_all()."""

    @pytest.mark.unit
    def test_returns_dict_with_all_four_keys(self):
        """extract_all() must return a dict with exactly the 4 expected table keys."""
        mock_df = pd.DataFrame({"col": [1, 2]})

        with patch.object(ingestion, "extract_product",          return_value=mock_df), \
             patch.object(ingestion, "extract_customer",          return_value=mock_df), \
             patch.object(ingestion, "extract_salesorderheader",  return_value=mock_df), \
             patch.object(ingestion, "extract_salesorderdetail",  return_value=mock_df):

            result = extract_all()

        expected_keys = {"product", "customer", "salesorderheader", "salesorderdetail"}
        assert set(result.keys()) == expected_keys

    @pytest.mark.unit
    def test_returns_dataframes_for_each_key(self):
        """Each value in extract_all() result must be a pandas DataFrame."""
        mock_df = pd.DataFrame({"id": [1], "name": ["test"]})

        with patch.object(ingestion, "extract_product",          return_value=mock_df), \
             patch.object(ingestion, "extract_customer",          return_value=mock_df), \
             patch.object(ingestion, "extract_salesorderheader",  return_value=mock_df), \
             patch.object(ingestion, "extract_salesorderdetail",  return_value=mock_df):

            result = extract_all()

        for key, df in result.items():
            assert isinstance(df, pd.DataFrame), f"Value for key '{key}' is not a DataFrame"


# ──────────────────────────────────────────────────────────────
# stage_raw TESTS
# ──────────────────────────────────────────────────────────────

class TestStageRaw:
    """Tests for stage_raw()."""

    @pytest.mark.unit
    def test_stage_raw_returns_path_dict(self, tmp_path):
        """stage_raw() must return a dict mapping table names to file Paths."""
        mock_df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        raw = {
            "product":          mock_df,
            "customer":         mock_df,
            "salesorderheader": mock_df,
            "salesorderdetail": mock_df,
        }

        # Patch STAGING_RAW to write to tmp_path instead of real staging/
        with patch.object(ingestion, "STAGING_RAW", tmp_path):
            with patch("ingestion.ensure_directories"):
                paths = stage_raw(raw)

        assert isinstance(paths, dict)
        assert set(paths.keys()) == {"product", "customer", "salesorderheader", "salesorderdetail"}

    @pytest.mark.unit
    def test_stage_raw_writes_parquet_files(self, tmp_path):
        """stage_raw() must write a .parquet file for each table."""
        mock_df = pd.DataFrame({"id": [1], "value": [100.0]})
        raw = {
            "product":          mock_df,
            "customer":         mock_df,
            "salesorderheader": mock_df,
            "salesorderdetail": mock_df,
        }

        with patch.object(ingestion, "STAGING_RAW", tmp_path):
            with patch("ingestion.ensure_directories"):
                stage_raw(raw)

        parquet_files = list(tmp_path.glob("*.parquet"))
        assert len(parquet_files) == 4, f"Expected 4 parquet files, found {len(parquet_files)}"
