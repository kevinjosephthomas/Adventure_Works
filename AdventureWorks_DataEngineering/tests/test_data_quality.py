# ============================================================
# tests/test_data_quality.py
# AdventureWorks Data Engineering Pipeline
#
# Unit tests for src/data_quality.py
# Tests the DQRule / DQResult dataclasses and individual
# rule-evaluator functions using synthetic DataFrames.
# ============================================================

import pytest
import pandas as pd
import numpy as np
from dataclasses import fields

# src/ is on sys.path from conftest.py
from data_quality import (
    DQRule,
    DQResult,
    RULE_ROW_COUNT_MIN,
    RULE_ROW_COUNT_MAX,
    RULE_NULL_RATIO_MAX,
    RULE_DUPE_RATIO_MAX,
    RULE_COLUMN_EXISTS,
    RULE_TOTAL_SALES_MIN,
    _check_row_count_min,
    _check_row_count_max,
    _check_null_ratio,
    _check_dupe_ratio,
    _check_column_exists,
    _check_total_sales_min,
    DEFAULT_RULES,
    run_data_quality_checks,
)


# ──────────────────────────────────────────────────────────────
# DATACLASS TESTS
# ──────────────────────────────────────────────────────────────

class TestDQRuleDataclass:
    """Tests for the DQRule dataclass."""

    @pytest.mark.unit
    def test_dqrule_instantiates_correctly(self):
        """DQRule must instantiate with all required fields."""
        rule = DQRule(
            rule_id="test_rule",
            table="product",
            rule_type=RULE_ROW_COUNT_MIN,
            threshold=100.0,
            severity="ERROR",
            description="Test rule: product must have 100+ rows",
        )
        assert rule.rule_id   == "test_rule"
        assert rule.table     == "product"
        assert rule.rule_type == RULE_ROW_COUNT_MIN
        assert rule.threshold == 100.0
        assert rule.severity  == "ERROR"

    @pytest.mark.unit
    def test_dqrule_column_is_optional(self):
        """DQRule.column should default to None when not provided."""
        rule = DQRule(
            rule_id="r1", table="t1", rule_type=RULE_ROW_COUNT_MIN,
            threshold=10, severity="WARNING", description="desc"
        )
        assert rule.column is None

    @pytest.mark.unit
    def test_dqrule_with_column(self):
        """DQRule.column should store the provided value."""
        rule = DQRule(
            rule_id="r2", table="product", rule_type=RULE_NULL_RATIO_MAX,
            threshold=0.01, severity="ERROR", description="null check",
            column="ProductID"
        )
        assert rule.column == "ProductID"


class TestDQResultDataclass:
    """Tests for the DQResult dataclass."""

    @pytest.mark.unit
    def test_dqresult_status_str_pass(self):
        """DQResult.status_str should return 'PASS' when passed=True."""
        result = DQResult(
            rule_id="r1", table="t", rule_type=RULE_ROW_COUNT_MIN,
            passed=True, severity="ERROR", actual_value=500,
            threshold=100, message="OK"
        )
        assert result.status_str == "PASS"

    @pytest.mark.unit
    def test_dqresult_status_str_fail_error(self):
        """DQResult.status_str should return 'FAIL-ERROR' when passed=False with ERROR severity."""
        result = DQResult(
            rule_id="r1", table="t", rule_type=RULE_ROW_COUNT_MIN,
            passed=False, severity="ERROR", actual_value=50,
            threshold=100, message="FAIL"
        )
        assert result.status_str == "FAIL-ERROR"

    @pytest.mark.unit
    def test_dqresult_to_dict_includes_status(self):
        """DQResult.to_dict() must include a 'status' key."""
        result = DQResult(
            rule_id="r1", table="t", rule_type=RULE_ROW_COUNT_MIN,
            passed=True, severity="WARNING", actual_value=200,
            threshold=100, message="OK"
        )
        d = result.to_dict()
        assert "status" in d
        assert d["status"] == "PASS"

    @pytest.mark.unit
    def test_dqresult_checked_at_auto_populated(self):
        """DQResult.checked_at should be auto-populated as an ISO timestamp string."""
        result = DQResult(
            rule_id="r1", table="t", rule_type=RULE_ROW_COUNT_MIN,
            passed=True, severity="ERROR", actual_value=1000,
            threshold=100, message="OK"
        )
        assert result.checked_at is not None
        assert len(result.checked_at) > 10  # basic ISO format sanity


# ──────────────────────────────────────────────────────────────
# ROW COUNT MIN TESTS
# ──────────────────────────────────────────────────────────────

class TestRowCountMin:
    """Tests for _check_row_count_min()."""

    def _make_rule(self, threshold: float) -> DQRule:
        return DQRule(
            rule_id="rc_min_test", table="product",
            rule_type=RULE_ROW_COUNT_MIN, threshold=threshold,
            severity="ERROR", description="Test row count min"
        )

    @pytest.mark.unit
    def test_passes_when_rows_meet_threshold(self):
        """Rule PASSES when actual rows >= threshold."""
        df = pd.DataFrame({"id": range(500)})
        rule = self._make_rule(threshold=100)
        result = _check_row_count_min(df, rule)
        assert result.passed is True

    @pytest.mark.unit
    def test_passes_when_rows_equal_threshold(self):
        """Rule PASSES when actual rows == threshold (boundary condition)."""
        df = pd.DataFrame({"id": range(100)})
        rule = self._make_rule(threshold=100)
        result = _check_row_count_min(df, rule)
        assert result.passed is True

    @pytest.mark.unit
    def test_fails_when_rows_below_threshold(self):
        """Rule FAILS when actual rows < threshold."""
        df = pd.DataFrame({"id": range(50)})
        rule = self._make_rule(threshold=100)
        result = _check_row_count_min(df, rule)
        assert result.passed is False
        assert result.actual_value == 50

    @pytest.mark.unit
    def test_result_has_correct_severity(self):
        """DQResult must carry the severity from the rule."""
        df = pd.DataFrame({"id": range(10)})
        rule = self._make_rule(threshold=1000)
        result = _check_row_count_min(df, rule)
        assert result.severity == "ERROR"


# ──────────────────────────────────────────────────────────────
# ROW COUNT MAX TESTS
# ──────────────────────────────────────────────────────────────

class TestRowCountMax:
    """Tests for _check_row_count_max()."""

    def _make_rule(self, threshold: float, severity="WARNING") -> DQRule:
        return DQRule(
            rule_id="rc_max_test", table="product",
            rule_type=RULE_ROW_COUNT_MAX, threshold=threshold,
            severity=severity, description="Test row count max"
        )

    @pytest.mark.unit
    def test_passes_when_rows_below_max(self):
        """Rule PASSES when rows <= threshold."""
        df = pd.DataFrame({"id": range(500)})
        result = _check_row_count_max(df, self._make_rule(10_000))
        assert result.passed is True

    @pytest.mark.unit
    def test_fails_when_rows_exceed_max(self):
        """Rule FAILS when rows > threshold."""
        df = pd.DataFrame({"id": range(200)})
        result = _check_row_count_max(df, self._make_rule(100))
        assert result.passed is False


# ──────────────────────────────────────────────────────────────
# NULL RATIO TESTS
# ──────────────────────────────────────────────────────────────

class TestNullRatio:
    """Tests for _check_null_ratio()."""

    def _make_rule(self, threshold: float, col="ProductID") -> DQRule:
        return DQRule(
            rule_id="null_test", table="product",
            rule_type=RULE_NULL_RATIO_MAX, threshold=threshold,
            severity="ERROR", description="null ratio test", column=col
        )

    @pytest.mark.unit
    def test_passes_when_no_nulls(self):
        """Rule PASSES when null ratio is 0 (all values present)."""
        df = pd.DataFrame({"ProductID": [1, 2, 3, 4, 5]})
        result = _check_null_ratio(df, self._make_rule(threshold=0.01))
        assert result.passed == True
        assert result.actual_value == 0.0

    @pytest.mark.unit
    def test_fails_when_null_ratio_exceeds_threshold(self):
        """Rule FAILS when null ratio > threshold."""
        # 2 out of 4 rows are null = 50% null ratio → threshold 0.01 → FAIL
        df = pd.DataFrame({"ProductID": [1, None, None, 4]})
        result = _check_null_ratio(df, self._make_rule(threshold=0.01))
        assert result.passed == False
        assert result.actual_value == pytest.approx(0.5)

    @pytest.mark.unit
    def test_passes_with_allowed_null_ratio(self):
        """Rule PASSES when null ratio is within the allowed threshold."""
        # 1 out of 100 rows null = 1% → threshold 0.01 → PASS (boundary)
        ids = list(range(99)) + [None]
        df = pd.DataFrame({"ProductID": ids})
        result = _check_null_ratio(df, self._make_rule(threshold=0.01))
        assert result.passed == True

    @pytest.mark.unit
    def test_column_not_found_fails(self):
        """Rule FAILS with actual_value=-1 when the column doesn't exist."""
        df = pd.DataFrame({"SomeOtherCol": [1, 2, 3]})
        result = _check_null_ratio(df, self._make_rule(threshold=0.01, col="ProductID"))
        assert result.passed == False
        assert result.actual_value == -1


# ──────────────────────────────────────────────────────────────
# DUPLICATE RATIO TESTS
# ──────────────────────────────────────────────────────────────

class TestDupeRatio:
    """Tests for _check_dupe_ratio()."""

    def _make_rule(self, threshold: float, severity="WARNING") -> DQRule:
        return DQRule(
            rule_id="dupe_test", table="product",
            rule_type=RULE_DUPE_RATIO_MAX, threshold=threshold,
            severity=severity, description="dupe ratio test", column="ProductID"
        )

    @pytest.mark.unit
    def test_passes_with_no_duplicates(self):
        """Rule PASSES when all values are unique."""
        df = pd.DataFrame({"ProductID": [1, 2, 3, 4, 5]})
        result = _check_dupe_ratio(df, self._make_rule(threshold=0.005))
        assert result.passed == True
        assert result.actual_value == 0.0

    @pytest.mark.unit
    def test_fails_when_dupe_ratio_exceeds_threshold(self):
        """Rule FAILS when duplicate ratio > threshold."""
        # 3 duplicates out of 6 rows = 50%
        df = pd.DataFrame({"ProductID": [1, 1, 1, 2, 2, 3]})
        result = _check_dupe_ratio(df, self._make_rule(threshold=0.005))
        assert result.passed == False

    @pytest.mark.unit
    def test_warning_severity_preserved(self):
        """WARNING severity rules should have FAIL-WARNING status when they fail."""
        df = pd.DataFrame({"ProductID": [1, 1, 2]})  # 33% duplicate ratio
        result = _check_dupe_ratio(df, self._make_rule(threshold=0.005, severity="WARNING"))
        assert result.passed == False
        assert result.status_str == "FAIL-WARNING"


# ──────────────────────────────────────────────────────────────
# COLUMN EXISTS TESTS
# ──────────────────────────────────────────────────────────────

class TestColumnExists:
    """Tests for _check_column_exists()."""

    def _make_rule(self, col: str, severity="ERROR") -> DQRule:
        return DQRule(
            rule_id="col_exists_test", table="product",
            rule_type=RULE_COLUMN_EXISTS, threshold=1.0,
            severity=severity, description=f"column {col} must exist", column=col
        )

    @pytest.mark.unit
    def test_passes_when_column_exists(self):
        """Rule PASSES when the required column is present."""
        df = pd.DataFrame({"ProductID": [1, 2], "Name": ["Bike", "Helmet"]})
        result = _check_column_exists(df, self._make_rule("Name"))
        assert result.passed is True
        assert result.actual_value == 1.0

    @pytest.mark.unit
    def test_fails_when_column_missing(self):
        """Rule FAILS with ERROR when the required column is absent."""
        df = pd.DataFrame({"ProductID": [1, 2]})
        result = _check_column_exists(df, self._make_rule("ListPrice", severity="ERROR"))
        assert result.passed is False
        assert result.severity == "ERROR"
        assert result.actual_value == 0.0


# ──────────────────────────────────────────────────────────────
# TOTAL SALES MIN TESTS
# ──────────────────────────────────────────────────────────────

class TestTotalSalesMin:
    """Tests for _check_total_sales_min()."""

    def _make_rule(self, threshold: float) -> DQRule:
        return DQRule(
            rule_id="sales_min_test", table="salesorderdetail",
            rule_type=RULE_TOTAL_SALES_MIN, threshold=threshold,
            severity="ERROR", description="total sales min", column="LineTotal"
        )

    @pytest.mark.unit
    def test_passes_when_total_exceeds_threshold(self):
        """Rule PASSES when sum(LineTotal) >= threshold."""
        df = pd.DataFrame({"LineTotal": [500_000.0, 600_000.0, 100_000.0]})
        result = _check_total_sales_min(df, self._make_rule(threshold=1_000_000))
        assert result.passed == True

    @pytest.mark.unit
    def test_fails_when_total_below_threshold(self):
        """Rule FAILS when sum(LineTotal) < threshold."""
        df = pd.DataFrame({"LineTotal": [100.0, 200.0]})
        result = _check_total_sales_min(df, self._make_rule(threshold=1_000_000))
        assert result.passed == False
        assert result.actual_value == pytest.approx(300.0)

    @pytest.mark.unit
    def test_missing_column_fails(self):
        """Rule FAILS when LineTotal column is absent."""
        df = pd.DataFrame({"OtherCol": [1, 2, 3]})
        result = _check_total_sales_min(df, self._make_rule(threshold=1_000_000))
        assert result.passed is False


# ──────────────────────────────────────────────────────────────
# DEFAULT RULES TESTS
# ──────────────────────────────────────────────────────────────

class TestDefaultRules:
    """Sanity checks on the DEFAULT_RULES list."""

    @pytest.mark.unit
    def test_default_rules_is_non_empty(self):
        """DEFAULT_RULES must contain at least one rule."""
        assert len(DEFAULT_RULES) > 0

    @pytest.mark.unit
    def test_all_rules_have_valid_severity(self):
        """Every rule in DEFAULT_RULES must have 'ERROR' or 'WARNING' severity."""
        for rule in DEFAULT_RULES:
            assert rule.severity in ("ERROR", "WARNING"), \
                f"Rule '{rule.rule_id}' has invalid severity: {rule.severity}"

    @pytest.mark.unit
    def test_all_rule_ids_are_unique(self):
        """All rule IDs in DEFAULT_RULES must be unique."""
        ids = [r.rule_id for r in DEFAULT_RULES]
        assert len(ids) == len(set(ids)), "Duplicate rule IDs found in DEFAULT_RULES"

    @pytest.mark.unit
    def test_column_exists_rules_have_column_set(self):
        """All COLUMN_EXISTS rules must have the 'column' field set."""
        for rule in DEFAULT_RULES:
            if rule.rule_type == RULE_COLUMN_EXISTS:
                assert rule.column is not None, \
                    f"COLUMN_EXISTS rule '{rule.rule_id}' must have 'column' set"
