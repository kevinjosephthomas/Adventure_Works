# ============================================================
# tests/test_pipeline_integration.py
# AdventureWorks Data Engineering Pipeline
#
# Integration/smoke tests for src/pipeline.py
# All heavy dependencies (extract_all, validate_all, transform_all,
# OLTP, OLAP, analytics) are mocked so tests run in milliseconds
# with no CSV files, no database, and no staging directories.
# ============================================================

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, call
from pathlib import Path


# ──────────────────────────────────────────────────────────────
# MOCK DATA HELPERS
# ──────────────────────────────────────────────────────────────

def _make_mock_raw():
    """Return a minimal raw dict mimicking extract_all() output."""
    df = pd.DataFrame({"id": [1, 2, 3]})
    return {
        "product":          df.copy(),
        "customer":         df.copy(),
        "salesorderheader": df.copy(),
        "salesorderdetail": df.copy(),
    }


def _make_mock_val_result(raw):
    """Return a minimal validation result dict mimicking validate_all() output."""
    return {
        "passed": True,
        "valid":  raw,   # for simplicity, valid == raw in tests
        "reports": {
            "product":          {"nulls": {"null_records": 0, "valid_records": 3},
                                  "duplicates": {"duplicate_records": 0},
                                  "outliers":   {"outlier_records": 0}},
            "customer":         {"nulls": {"null_records": 0, "valid_records": 3},
                                  "duplicates": {"duplicate_records": 0},
                                  "outliers":   {"outlier_records": 0}},
            "salesorderheader": {"nulls": {"null_records": 0, "valid_records": 3},
                                  "duplicates": {"duplicate_records": 0},
                                  "outliers":   {"outlier_records": 0}},
            "salesorderdetail": {"nulls": {"null_records": 0, "valid_records": 3},
                                  "duplicates": {"duplicate_records": 0},
                                  "outliers":   {"outlier_records": 0}},
        },
    }


def _make_mock_transformed():
    """Return a minimal transformed dict mimicking transform_all() output."""
    df = pd.DataFrame({"id": [1, 2, 3]})
    return {
        "product":      df.copy(),
        "customer":     df.copy(),
        "sales":        df.copy(),
        "aggregations": {},
    }


def _make_mock_olap():
    """Return a minimal OLAP dict mimicking build_olap() output."""
    df3  = pd.DataFrame({"id": [1, 2, 3]})
    df30 = pd.DataFrame({"id": range(30)})
    return {
        "dim_date":     df3.copy(),
        "dim_product":  df3.copy(),
        "dim_customer": df3.copy(),
        "fact_sales":   df30,
    }


# ──────────────────────────────────────────────────────────────
# TESTS
# ──────────────────────────────────────────────────────────────

class TestRunPipeline:
    """Integration/smoke tests for run_pipeline()."""

    @pytest.mark.integration
    def test_returns_dict_with_expected_keys(self):
        """
        run_pipeline() must return a dict containing all required
        report keys when run with db_load=False and mocked I/O.
        """
        mock_raw         = _make_mock_raw()
        mock_val_result  = _make_mock_val_result(mock_raw)
        mock_transformed = _make_mock_transformed()
        mock_olap        = _make_mock_olap()
        mock_analytics   = {"kpis": {"total_sales": 9000.0}, "datasets": {"a": 1}}

        with patch("pipeline.extract_all",      return_value=mock_raw),          \
             patch("pipeline.stage_raw",         return_value={}),                \
             patch("pipeline.validate_all",      return_value=mock_val_result),   \
             patch("pipeline.transform_all",     return_value=mock_transformed),  \
             patch("pipeline.get_engine",        return_value=None),              \
             patch("pipeline.build_olap",        return_value=mock_olap),         \
             patch("pipeline.load_olap_to_db",   return_value=None),              \
             patch("pipeline.run_analytics",     return_value=mock_analytics),    \
             patch("pipeline.ensure_directories",return_value=None),              \
             patch("pipeline.PIPELINE_LOG_FILE", Path("nul")):                   \

            from pipeline import run_pipeline
            report = run_pipeline(db_load=False)

        assert isinstance(report, dict), "run_pipeline() must return a dict"

    @pytest.mark.integration
    def test_pipeline_report_contains_required_fields(self):
        """
        The returned report dict must contain all 14 standard fields
        required by the project specification.
        """
        mock_raw         = _make_mock_raw()
        mock_val_result  = _make_mock_val_result(mock_raw)
        mock_transformed = _make_mock_transformed()
        mock_olap        = _make_mock_olap()
        mock_analytics   = {"kpis": {}, "datasets": {}}

        required_fields = [
            "run_id", "start_time", "end_time", "duration_sec", "mode",
            "product_source_records", "customer_source_records",
            "header_source_records", "detail_source_records",
            "valid_records", "invalid_records", "duplicate_records",
            "oltp_records", "factsales_records", "status",
        ]

        with patch("pipeline.extract_all",      return_value=mock_raw),          \
             patch("pipeline.stage_raw",         return_value={}),                \
             patch("pipeline.validate_all",      return_value=mock_val_result),   \
             patch("pipeline.transform_all",     return_value=mock_transformed),  \
             patch("pipeline.get_engine",        return_value=None),              \
             patch("pipeline.build_olap",        return_value=mock_olap),         \
             patch("pipeline.load_olap_to_db",   return_value=None),              \
             patch("pipeline.run_analytics",     return_value=mock_analytics),    \
             patch("pipeline.ensure_directories",return_value=None),              \
             patch("pipeline.PIPELINE_LOG_FILE", Path("nul")):

            from pipeline import run_pipeline
            report = run_pipeline(db_load=False)

        for field in required_fields:
            assert field in report, f"Missing required field '{field}' in pipeline report"

    @pytest.mark.integration
    def test_pipeline_status_is_success_on_clean_run(self):
        """
        Pipeline status must be 'SUCCESS' when all steps complete
        without raising exceptions.
        """
        mock_raw         = _make_mock_raw()
        mock_val_result  = _make_mock_val_result(mock_raw)
        mock_transformed = _make_mock_transformed()
        mock_olap        = _make_mock_olap()
        mock_analytics   = {"kpis": {}, "datasets": {}}

        with patch("pipeline.extract_all",      return_value=mock_raw),          \
             patch("pipeline.stage_raw",         return_value={}),                \
             patch("pipeline.validate_all",      return_value=mock_val_result),   \
             patch("pipeline.transform_all",     return_value=mock_transformed),  \
             patch("pipeline.get_engine",        return_value=None),              \
             patch("pipeline.build_olap",        return_value=mock_olap),         \
             patch("pipeline.load_olap_to_db",   return_value=None),              \
             patch("pipeline.run_analytics",     return_value=mock_analytics),    \
             patch("pipeline.ensure_directories",return_value=None),              \
             patch("pipeline.PIPELINE_LOG_FILE", Path("nul")):

            from pipeline import run_pipeline
            report = run_pipeline(db_load=False)

        assert report["status"] == "SUCCESS", \
            f"Expected pipeline status 'SUCCESS', got '{report['status']}'"

    @pytest.mark.integration
    def test_pipeline_status_is_failed_on_extraction_error(self):
        """
        Pipeline status must be 'FAILED' when extract_all() raises an exception.
        The exception should be caught gracefully — pipeline must NOT crash the process.
        """
        with patch("pipeline.extract_all",       side_effect=FileNotFoundError("CSV not found")), \
             patch("pipeline.ensure_directories", return_value=None),                               \
             patch("pipeline.PIPELINE_LOG_FILE",  Path("nul")):

            from pipeline import run_pipeline
            report = run_pipeline(db_load=False)

        assert report["status"] == "FAILED", \
            "Pipeline should report FAILED when extraction raises an exception"
        assert "CSV not found" in report.get("error_message", ""), \
            "Error message should contain the original exception text"

    @pytest.mark.integration
    def test_pipeline_source_record_counts_match_mock(self):
        """
        Product/Customer/Header/Detail record counts in report must
        match the sizes of the DataFrames returned by extract_all().
        """
        mock_raw = {
            "product":          pd.DataFrame({"id": range(10)}),
            "customer":         pd.DataFrame({"id": range(20)}),
            "salesorderheader": pd.DataFrame({"id": range(30)}),
            "salesorderdetail": pd.DataFrame({"id": range(40)}),
        }
        mock_val_result  = _make_mock_val_result(mock_raw)
        mock_transformed = _make_mock_transformed()
        mock_olap        = _make_mock_olap()
        mock_analytics   = {"kpis": {}, "datasets": {}}

        with patch("pipeline.extract_all",      return_value=mock_raw),          \
             patch("pipeline.stage_raw",         return_value={}),                \
             patch("pipeline.validate_all",      return_value=mock_val_result),   \
             patch("pipeline.transform_all",     return_value=mock_transformed),  \
             patch("pipeline.get_engine",        return_value=None),              \
             patch("pipeline.build_olap",        return_value=mock_olap),         \
             patch("pipeline.load_olap_to_db",   return_value=None),              \
             patch("pipeline.run_analytics",     return_value=mock_analytics),    \
             patch("pipeline.ensure_directories",return_value=None),              \
             patch("pipeline.PIPELINE_LOG_FILE", Path("nul")):

            from pipeline import run_pipeline
            report = run_pipeline(db_load=False)

        assert report["product_source_records"]  == 10
        assert report["customer_source_records"] == 20
        assert report["header_source_records"]   == 30
        assert report["detail_source_records"]   == 40

    @pytest.mark.integration
    def test_pipeline_duration_is_positive(self):
        """
        duration_sec in the report must be a positive float
        (pipeline took some non-zero time to run).
        """
        mock_raw         = _make_mock_raw()
        mock_val_result  = _make_mock_val_result(mock_raw)
        mock_transformed = _make_mock_transformed()
        mock_olap        = _make_mock_olap()
        mock_analytics   = {"kpis": {}, "datasets": {}}

        with patch("pipeline.extract_all",      return_value=mock_raw),          \
             patch("pipeline.stage_raw",         return_value={}),                \
             patch("pipeline.validate_all",      return_value=mock_val_result),   \
             patch("pipeline.transform_all",     return_value=mock_transformed),  \
             patch("pipeline.get_engine",        return_value=None),              \
             patch("pipeline.build_olap",        return_value=mock_olap),         \
             patch("pipeline.load_olap_to_db",   return_value=None),              \
             patch("pipeline.run_analytics",     return_value=mock_analytics),    \
             patch("pipeline.ensure_directories",return_value=None),              \
             patch("pipeline.PIPELINE_LOG_FILE", Path("nul")):

            from pipeline import run_pipeline
            report = run_pipeline(db_load=False)

        assert isinstance(report["duration_sec"], float)
        assert report["duration_sec"] >= 0.0
