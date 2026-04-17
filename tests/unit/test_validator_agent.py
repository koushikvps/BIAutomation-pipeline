"""Tests for Validator Agent."""

import json
import pytest
from unittest.mock import patch, MagicMock

from validator.agent import ValidatorAgent, SYNAPSE_BLOCKLIST
from shared.models import (
    ArtifactBundle, ArtifactType, BuildPlan, BuildStep,
    GeneratedArtifact, Layer, ValidationStatus,
)


class TestValidatorAgent:
    @pytest.fixture
    def agent(self, mock_config):
        with patch("validator.agent.SynapseClient") as mock_syn_cls:
            mock_syn = MagicMock()
            mock_syn.execute_query.return_value = [{"cnt": 100}]
            mock_syn_cls.return_value = mock_syn
            return ValidatorAgent(mock_config)

    @pytest.fixture
    def sample_bundle(self):
        return ArtifactBundle(
            story_id="TEST-001",
            artifacts=[
                GeneratedArtifact(
                    step=1, artifact_type=ArtifactType.EXTERNAL_TABLE,
                    object_name="[bronze].[ext_Sales]", layer=Layer.BRONZE,
                    file_name="bronze/ext_Sales.sql",
                    content="CREATE EXTERNAL TABLE [bronze].[ext_Sales] (id int) WITH (LOCATION='sales/');"
                ),
                GeneratedArtifact(
                    step=2, artifact_type=ArtifactType.VIEW,
                    object_name="[gold].[vw_sales_summary]", layer=Layer.GOLD,
                    file_name="gold/vw_sales_summary.sql",
                    content="CREATE VIEW [gold].[vw_sales_summary] AS SELECT id FROM [silver].[Sales];"
                ),
            ],
        )

    @pytest.fixture
    def sample_plan(self):
        return BuildPlan(
            story_id="TEST-001", mode="greenfield", risk_level="low",
            execution_order=[
                {"step": 1, "layer": "bronze", "action": "create",
                 "artifact_type": "external_table", "object_name": "[bronze].[ext_Sales]",
                 "columns": [], "logic_summary": "test", "depends_on": []},
                {"step": 2, "layer": "gold", "action": "create",
                 "artifact_type": "view", "object_name": "[gold].[vw_sales_summary]",
                 "columns": [], "logic_summary": "test", "depends_on": [1]},
            ],
            validation_requirements=[],
        )

    def test_pre_deploy_returns_report(self, agent, sample_bundle, sample_plan):
        report = agent.pre_deploy_check(sample_bundle, sample_plan)
        assert report.story_id == "TEST-001"
        assert report.phase == "pre_deploy"
        assert len(report.checks) > 0

    def test_naming_passes_for_correct_names(self, agent, sample_bundle, sample_plan):
        report = agent.pre_deploy_check(sample_bundle, sample_plan)
        naming_checks = [c for c in report.checks if c.check_type == "naming_convention"]
        assert len(naming_checks) == 2
        assert all(c.status == ValidationStatus.PASS for c in naming_checks)

    def test_naming_fails_for_bad_name(self, agent, sample_plan):
        bad_bundle = ArtifactBundle(
            story_id="TEST-001",
            artifacts=[
                GeneratedArtifact(
                    step=1, artifact_type=ArtifactType.VIEW,
                    object_name="bad_name_no_schema", layer=Layer.GOLD,
                    file_name="gold/bad.sql",
                    content="CREATE VIEW bad_name_no_schema AS SELECT 1;"
                ),
            ],
        )
        report = agent.pre_deploy_check(bad_bundle, sample_plan)
        naming_checks = [c for c in report.checks if c.check_type == "naming_convention"]
        assert any(c.status == ValidationStatus.FAIL for c in naming_checks)

    def test_sql_syntax_detects_use_statement(self, agent, sample_plan):
        bad_bundle = ArtifactBundle(
            story_id="TEST-001",
            artifacts=[
                GeneratedArtifact(
                    step=1, artifact_type=ArtifactType.TABLE,
                    object_name="[bronze].[test]", layer=Layer.BRONZE,
                    file_name="bronze/test.sql",
                    content="USE mydb;\nCREATE TABLE [bronze].[test] (id int);"
                ),
            ],
        )
        report = agent.pre_deploy_check(bad_bundle, sample_plan)
        syntax_checks = [c for c in report.checks if c.check_type == "sql_syntax"]
        use_check = [c for c in syntax_checks if "USE" in c.message]
        assert len(use_check) > 0
        assert use_check[0].status == ValidationStatus.FAIL

    def test_sql_syntax_detects_select_star(self, agent, sample_plan):
        bad_bundle = ArtifactBundle(
            story_id="TEST-001",
            artifacts=[
                GeneratedArtifact(
                    step=1, artifact_type=ArtifactType.VIEW,
                    object_name="[gold].[vw_test]", layer=Layer.GOLD,
                    file_name="gold/vw_test.sql",
                    content="CREATE VIEW [gold].[vw_test] AS SELECT * FROM [silver].[test];"
                ),
            ],
        )
        report = agent.pre_deploy_check(bad_bundle, sample_plan)
        syntax_checks = [c for c in report.checks if c.check_type == "sql_syntax"]
        star_check = [c for c in syntax_checks if "SELECT *" in c.message]
        assert len(star_check) > 0

    def test_adf_json_valid(self, agent, sample_plan):
        adf_bundle = ArtifactBundle(
            story_id="TEST-001",
            artifacts=[
                GeneratedArtifact(
                    step=1, artifact_type=ArtifactType.ADF_PIPELINE,
                    object_name="pl_test", layer=Layer.BRONZE,
                    file_name="adf/pl_test.json",
                    content='{"name": "pl_test", "properties": {"activities": []}}'
                ),
            ],
        )
        report = agent.pre_deploy_check(adf_bundle, sample_plan)
        json_checks = [c for c in report.checks if c.check_type == "json_syntax"]
        assert len(json_checks) == 1
        assert json_checks[0].status == ValidationStatus.PASS

    def test_adf_json_invalid(self, agent, sample_plan):
        adf_bundle = ArtifactBundle(
            story_id="TEST-001",
            artifacts=[
                GeneratedArtifact(
                    step=1, artifact_type=ArtifactType.ADF_PIPELINE,
                    object_name="pl_test", layer=Layer.BRONZE,
                    file_name="adf/pl_test.json",
                    content='{invalid json}'
                ),
            ],
        )
        report = agent.pre_deploy_check(adf_bundle, sample_plan)
        json_checks = [c for c in report.checks if c.check_type == "json_syntax"]
        assert json_checks[0].status == ValidationStatus.FAIL

    def test_dependency_check(self, agent, sample_bundle, sample_plan):
        report = agent.pre_deploy_check(sample_bundle, sample_plan)
        dep_checks = [c for c in report.checks if c.check_type == "dependency"]
        assert len(dep_checks) >= 1

    def test_overall_status_pass_when_clean(self, agent, sample_bundle, sample_plan):
        report = agent.pre_deploy_check(sample_bundle, sample_plan)
        assert report.overall_status in (ValidationStatus.PASS, ValidationStatus.WARN)

    def test_overall_status_fail_when_issues(self, agent, sample_plan):
        bad_bundle = ArtifactBundle(
            story_id="TEST-001",
            artifacts=[
                GeneratedArtifact(
                    step=1, artifact_type=ArtifactType.VIEW,
                    object_name="no_schema", layer=Layer.GOLD,
                    file_name="test.sql",
                    content="USE mydb;\nSELECT * FROM bad;"
                ),
            ],
        )
        report = agent.pre_deploy_check(bad_bundle, sample_plan)
        assert report.overall_status == ValidationStatus.FAIL
        assert len(report.blocking_failures) > 0
