"""Tests for Bug Fixer Agent."""

import pytest
from unittest.mock import patch, MagicMock

from fixer.agent import BugFixerAgent


class TestBugFixerAgent:
    @pytest.fixture
    def agent(self, mock_config):
        with patch("fixer.agent.LLMClient") as mock_llm_cls, \
             patch("fixer.agent.SynapseClient") as mock_syn_cls:
            mock_llm = MagicMock()
            mock_llm.chat_json.return_value = {
                "fix_type": "data_fix",
                "root_cause": "Missing WHERE filter on cancelled orders",
                "corrected_artifacts": [
                    {
                        "object_name": "[gold].[vw_sales]",
                        "layer": "gold",
                        "artifact_type": "view",
                        "file_name": "gold/vw_sales.sql",
                        "content": "CREATE VIEW [gold].[vw_sales] AS SELECT * FROM [silver].[sales] WHERE status <> 'Cancelled';"
                    }
                ],
                "change_summary": "Added WHERE clause to filter cancelled orders",
                "confidence": "high",
                "recommendation": "",
            }
            mock_llm_cls.return_value = mock_llm

            mock_syn = MagicMock()
            mock_syn.get_columns.return_value = []
            mock_syn_cls.return_value = mock_syn

            agent = BugFixerAgent(mock_config)
            agent._llm = mock_llm
            agent._synapse = mock_syn
            return agent

    def test_analyze_returns_required_fields(self, agent):
        bug = {"id": 123, "title": "Sales report includes cancelled orders",
               "description": "The gold view shows cancelled orders that should be filtered out"}
        result = agent.analyze_and_fix(bug)
        assert "bug_id" in result
        assert "fix_type" in result
        assert "root_cause" in result
        assert "corrected_artifacts" in result
        assert "confidence" in result

    def test_fix_type_data_fix(self, agent):
        bug = {"id": 123, "title": "Wrong totals in gold view"}
        result = agent.analyze_and_fix(bug)
        assert result["fix_type"] == "data_fix"
        assert len(result["corrected_artifacts"]) == 1

    def test_fix_with_original_artifacts(self, agent):
        bug = {"id": 456, "title": "Join missing in silver table"}
        original = [{"object_name": "[silver].[sales_summary]", "layer": "silver",
                     "artifact_type": "table", "content": "CREATE TABLE ..."}]
        result = agent.analyze_and_fix(bug, original_artifacts=original)
        assert result["bug_id"] == 456
        agent._llm.chat_json.assert_called_once()

    def test_pipeline_fix_type(self, agent):
        agent._llm.chat_json.return_value = {
            "fix_type": "pipeline_fix",
            "root_cause": "Wrong source dataset reference",
            "corrected_artifacts": [{"object_name": "pl_bronze", "layer": "bronze",
                                     "artifact_type": "adf_pipeline", "content": "{}"}],
            "change_summary": "Fixed dataset reference",
            "confidence": "medium",
            "recommendation": "Verify linked service",
        }
        bug = {"id": 789, "title": "ADF pipeline failing"}
        result = agent.analyze_and_fix(bug)
        assert result["fix_type"] == "pipeline_fix"
        assert result["confidence"] == "medium"

    def test_ui_recommendation_type(self, agent):
        agent._llm.chat_json.return_value = {
            "fix_type": "ui_recommendation",
            "root_cause": "Form validation missing",
            "corrected_artifacts": [],
            "change_summary": "Add required field validation",
            "confidence": "high",
            "recommendation": "Add IsBlank() check on OrderDate field in Power App",
        }
        bug = {"id": 101, "title": "Can submit form without date"}
        result = agent.analyze_and_fix(bug)
        assert result["fix_type"] == "ui_recommendation"
        assert "Power App" in result["recommendation"]

    def test_catalog_context_extracts_schema_object(self, agent):
        bug = {"id": 1, "title": "Error in [gold].[vw_report]",
               "description": "Query on [silver].[orders] fails",
               "error_message": "Invalid column in [bronze].[ext_sales]"}
        agent._synapse.get_columns.return_value = [
            {"COLUMN_NAME": "id", "DATA_TYPE": "int"}
        ]
        ctx = agent._get_catalog_context(bug)
        assert "related_objects" in ctx
        # Should have tried to get columns for extracted objects
        assert agent._synapse.get_columns.call_count >= 1

    def test_handles_empty_bug_details(self, agent):
        result = agent.analyze_and_fix({})
        assert result["bug_id"] == "unknown"
        assert result["bug_title"] == ""
