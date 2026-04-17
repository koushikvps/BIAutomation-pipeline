"""Tests for StoryMapper."""

import pytest
from unittest.mock import MagicMock, patch

from shared.story_mapper import StoryMapper, strip_html


class TestStripHtml:
    def test_removes_tags(self):
        assert "Hello" in strip_html("<div><p>Hello</p></div>")

    def test_br_to_newline(self):
        result = strip_html("line1<br>line2")
        assert "line1" in result
        assert "line2" in result

    def test_empty_input(self):
        assert strip_html("") == ""
        assert strip_html(None) == ""

    def test_preserves_plain_text(self):
        assert strip_html("plain text") == "plain text"


class TestStoryMapperRouting:
    def test_detects_gherkin(self):
        mapper = StoryMapper(config=None)
        text = "Given I have sales data\nWhen I aggregate by region\nThen I see totals"
        assert mapper._needs_universal_interpreter(text) is True

    def test_detects_business_language(self):
        mapper = StoryMapper(config=None)
        text = "I want to see a dashboard that helps me analyze and track sales by region. I need to understand trends."
        assert mapper._needs_universal_interpreter(text) is True

    def test_technical_stays_rule_based(self):
        mapper = StoryMapper(config=None)
        text = "Create gold.vw_sales from dbo.SalesTransactions joined with dbo.Products on ProductID"
        assert mapper._needs_universal_interpreter(text) is False


class TestStoryMapperRuleBased:
    @patch("shared.story_mapper.LLMClient")
    def test_rule_based_extracts_tables(self, mock_llm_cls, mock_config):
        mock_llm_cls.return_value = MagicMock()
        mapper = StoryMapper(mock_config)
        result = mapper.map_work_item({
            "id": 1,
            "title": "Sales Summary",
            "description": "Build from dbo.SalesTransactions and dbo.Products",
            "acceptance_criteria": "",
            "priority": "2",
        })
        assert result is not None
        assert isinstance(result, dict)
        assert "story_id" in result
