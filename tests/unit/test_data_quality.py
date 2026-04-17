"""Tests for Data Quality Framework."""

import pytest
from unittest.mock import patch, MagicMock

from shared.data_quality import (
    DataQualityValidator, DQReport, DQResult,
    DQCheckType, DQStatus,
)


class TestDQReport:
    def test_empty_report(self):
        r = DQReport(story_id="TEST-001")
        assert r.pass_count == 0
        assert r.warn_count == 0
        assert r.fail_count == 0
        assert r.overall_status == DQStatus.PASS

    def test_pass_report(self):
        r = DQReport(story_id="TEST-001", results=[
            DQResult(DQCheckType.ROW_COUNT, "[bronze].[t1]", DQStatus.PASS, "100 rows"),
            DQResult(DQCheckType.NULL_CHECK, "[bronze].[t1]", DQStatus.PASS, "OK"),
        ])
        assert r.pass_count == 2
        assert r.overall_status == DQStatus.PASS

    def test_fail_overrides_warn(self):
        r = DQReport(story_id="TEST-001", results=[
            DQResult(DQCheckType.ROW_COUNT, "[bronze].[t1]", DQStatus.PASS, "OK"),
            DQResult(DQCheckType.NULL_CHECK, "[bronze].[t1]", DQStatus.WARN, "Empty"),
            DQResult(DQCheckType.DUPLICATE_CHECK, "[silver].[t1]", DQStatus.FAIL, "Dupes"),
        ])
        assert r.overall_status == DQStatus.FAIL
        assert r.fail_count == 1
        assert r.warn_count == 1

    def test_to_dict(self):
        r = DQReport(story_id="TEST-001", results=[
            DQResult(DQCheckType.ROW_COUNT, "[bronze].[t1]", DQStatus.PASS, "100 rows", ">0", "100"),
        ])
        d = r.to_dict()
        assert d["story_id"] == "TEST-001"
        assert d["overall_status"] == "pass"
        assert len(d["results"]) == 1
        assert d["results"][0]["check_type"] == "row_count"


class TestDataQualityValidator:
    @pytest.fixture
    def validator(self, mock_config):
        with patch("shared.data_quality.SynapseClient") as mock_syn_cls:
            mock_syn = MagicMock()
            mock_syn_cls.return_value = mock_syn
            v = DataQualityValidator(mock_config)
            v._synapse = mock_syn
            return v

    def test_skips_adf_objects(self, validator):
        objects = [{"schema": "bronze", "name": "pl_load", "layer": "bronze", "type": "adf_pipeline"}]
        report = validator.run_checks("TEST-001", objects)
        assert len(report.results) == 0

    def test_row_count_pass(self, validator):
        validator._synapse.execute_query.return_value = [{"cnt": 100}]
        objects = [{"schema": "bronze", "name": "sales", "layer": "bronze", "type": "table"}]
        report = validator.run_checks("TEST-001", objects)
        row_checks = [r for r in report.results if r.check_type == DQCheckType.ROW_COUNT]
        assert len(row_checks) == 1
        assert row_checks[0].status == DQStatus.PASS

    def test_row_count_warn_when_empty(self, validator):
        validator._synapse.execute_query.return_value = [{"cnt": 0}]
        objects = [{"schema": "bronze", "name": "sales", "layer": "bronze", "type": "table"}]
        report = validator.run_checks("TEST-001", objects)
        row_checks = [r for r in report.results if r.check_type == DQCheckType.ROW_COUNT]
        assert row_checks[0].status == DQStatus.WARN

    def test_null_check_pass(self, validator):
        validator._synapse.execute_query.return_value = [{"cnt": 100}]
        validator._synapse.get_columns.return_value = [
            {"COLUMN_NAME": "id", "IS_NULLABLE": "NO"},
        ]
        objects = [{"schema": "silver", "name": "orders", "layer": "silver", "type": "table"}]
        report = validator.run_checks("TEST-001", objects)
        null_checks = [r for r in report.results if r.check_type == DQCheckType.NULL_CHECK]
        assert len(null_checks) == 1

    def test_duplicate_check_on_silver_gold(self, validator):
        validator._synapse.execute_query.side_effect = [
            [{"cnt": 50}],  # row count
            [{"cnt": 50}],  # null count query  
            [],             # duplicate query (no dupes)
        ]
        validator._synapse.get_columns.return_value = [
            {"COLUMN_NAME": "order_id", "IS_NULLABLE": "YES"},
        ]
        objects = [{"schema": "silver", "name": "orders", "layer": "silver", "type": "table"}]
        report = validator.run_checks("TEST-001", objects)
        dup_checks = [r for r in report.results if r.check_type == DQCheckType.DUPLICATE_CHECK]
        assert len(dup_checks) == 1

    def test_no_duplicate_check_on_bronze(self, validator):
        validator._synapse.execute_query.return_value = [{"cnt": 100}]
        validator._synapse.get_columns.return_value = []
        objects = [{"schema": "bronze", "name": "raw", "layer": "bronze", "type": "table"}]
        report = validator.run_checks("TEST-001", objects)
        dup_checks = [r for r in report.results if r.check_type == DQCheckType.DUPLICATE_CHECK]
        assert len(dup_checks) == 0

    def test_handles_synapse_error(self, validator):
        validator._synapse.execute_query.side_effect = Exception("Synapse paused")
        objects = [{"schema": "bronze", "name": "test", "layer": "bronze", "type": "table"}]
        report = validator.run_checks("TEST-001", objects)
        skip_checks = [r for r in report.results if r.status == DQStatus.SKIP]
        assert len(skip_checks) >= 1
