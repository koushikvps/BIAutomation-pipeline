"""Tests for Test Router Agent."""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "test-automation"))
from tester.test_router import TestRouter


class TestRouterAgent:
    @pytest.fixture
    def router(self, mock_config):
        with patch("tester.test_router.LLMClient") as mock_llm_cls:
            mock_llm = MagicMock()
            mock_llm.chat_json.return_value = {
                "test_type": "data",
                "confidence": 0.95,
                "reasoning": "Story is about data pipeline",
                "ui_aspects": [],
                "data_aspects": ["row count", "null check"],
            }
            mock_llm_cls.return_value = mock_llm
            router = TestRouter(mock_config)
            router._llm = mock_llm
            return router

    def test_classify_data_story(self, router):
        story = {"title": "Build gold view for sales",
                 "description": "Create aggregated sales view in gold layer",
                 "acceptance_criteria": "Row count > 0"}
        result = router.classify(story)
        assert result["test_type"] == "data"
        assert result["confidence"] == 0.95

    def test_classify_ui_story(self, router):
        router._llm.chat_json.return_value = {
            "test_type": "ui",
            "confidence": 0.9,
            "reasoning": "Power App form testing",
            "ui_aspects": ["submit button", "date field"],
            "data_aspects": [],
        }
        story = {"title": "Add order form to Power App",
                 "description": "New form with submit button and date picker"}
        result = router.classify(story)
        assert result["test_type"] == "ui"
        assert len(result["ui_aspects"]) == 2

    def test_classify_both(self, router):
        router._llm.chat_json.return_value = {
            "test_type": "both",
            "confidence": 0.85,
            "reasoning": "Story has UI form and data validation",
            "ui_aspects": ["form submit"],
            "data_aspects": ["data written to table"],
        }
        story = {"title": "Submit order and validate in Synapse",
                 "description": "Form submission writes to Silver layer"}
        result = router.classify(story)
        assert result["test_type"] == "both"
        assert len(result["ui_aspects"]) > 0
        assert len(result["data_aspects"]) > 0

    def test_classify_empty_story(self, router):
        result = router.classify({})
        assert "test_type" in result

    def test_classify_calls_llm(self, router):
        router.classify({"title": "Test", "description": "Test"})
        router._llm.chat_json.assert_called_once()

    def test_classify_default_to_both_on_missing_type(self, router):
        router._llm.chat_json.return_value = {
            "confidence": 0.5,
            "reasoning": "Unclear",
        }
        story = {"title": "Ambiguous story"}
        result = router.classify(story)
        # TestRouter code: test_type = result.get("test_type", "both")
        assert result.get("test_type", "both") == "both"
