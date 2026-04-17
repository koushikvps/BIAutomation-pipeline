"""Tests for Code Review Agent."""

import pytest
from unittest.mock import patch, MagicMock

from reviewer.agent import CodeReviewAgent


class TestCodeReviewAgent:
    @pytest.fixture
    def agent(self, mock_config):
        with patch("reviewer.agent.LLMClient") as mock_llm_cls:
            mock_llm = MagicMock()
            mock_llm.chat_json.return_value = {
                "overall_verdict": "APPROVE",
                "reviews": [],
                "total_findings": 0,
                "critical_count": 0,
                "warning_count": 0,
                "info_count": 0,
                "review_summary": "All clear",
            }
            mock_llm_cls.return_value = mock_llm
            return CodeReviewAgent(mock_config)

    def test_review_returns_required_fields(self, agent):
        result = agent.review(
            artifacts=[{"object_name": "gold.test", "layer": "gold",
                       "artifact_type": "view", "content": "CREATE VIEW gold.test AS SELECT 1"}],
            build_plan={"execution_order": []},
        )
        assert "overall_verdict" in result
        assert "reviews" in result
        assert "total_findings" in result
        assert "critical_count" in result

    def test_review_empty_artifacts(self, agent):
        result = agent.review(artifacts=[], build_plan={})
        assert result["overall_verdict"] == "APPROVE"

    def test_verdict_override_on_critical(self, mock_config):
        with patch("reviewer.agent.LLMClient") as mock_llm_cls:
            mock_llm = MagicMock()
            mock_llm.chat_json.return_value = {
                "overall_verdict": "APPROVE",
                "reviews": [{
                    "artifact_name": "test",
                    "verdict": "REJECT",
                    "findings": [{"category": "security", "severity": "critical",
                                  "issue": "SQL injection", "fix": "Use params"}],
                }],
            }
            mock_llm_cls.return_value = mock_llm
            agent = CodeReviewAgent(mock_config)
            result = agent.review(
                artifacts=[{"object_name": "test", "layer": "gold",
                           "artifact_type": "view", "content": "SELECT 1"}],
                build_plan={},
            )
            assert result["overall_verdict"] == "REJECT"
            assert result["critical_count"] == 1
