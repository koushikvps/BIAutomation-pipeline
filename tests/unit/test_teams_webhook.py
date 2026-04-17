"""Tests for Teams Webhook."""

import pytest
from unittest.mock import patch, MagicMock

from shared.teams_webhook import (
    pipeline_started_card,
    review_gate_card,
    progress_card,
    completion_card,
)


class TestPipelineStartedCard:
    def test_returns_adaptive_card(self):
        card = pipeline_started_card(
            story_id="TEST-001", title="Daily Sales",
            tables=["dbo.Sales", "dbo.Products"], work_item_id="123",
        )
        assert card["type"] == "AdaptiveCard"
        assert card["version"] == "1.4"

    def test_body_is_list(self):
        card = pipeline_started_card(
            story_id="TEST-001", title="Sales Report",
            tables=["dbo.Sales"],
        )
        assert isinstance(card["body"], list)
        assert len(card["body"]) > 0

    def test_includes_story_info_in_body(self):
        card = pipeline_started_card(
            story_id="MY-STORY-99", title="Revenue Report",
            tables=["dbo.Revenue"],
        )
        body_str = str(card["body"])
        assert "MY-STORY-99" in body_str or "Revenue Report" in body_str


class TestReviewGateCard:
    def test_returns_adaptive_card(self):
        card = review_gate_card(
            instance_id="abc-123", mode="greenfield",
            risk_level="low", artifact_count=5,
            plan_summary=[{"step": 1, "object": "test"}],
        )
        assert card["type"] == "AdaptiveCard"
        body_str = str(card)
        assert "abc-123" in body_str or "greenfield" in body_str

    def test_has_actions(self):
        card = review_gate_card(
            instance_id="abc-123", mode="greenfield",
            risk_level="low", artifact_count=5,
            plan_summary=[],
        )
        assert "actions" in card
        assert len(card["actions"]) >= 2


class TestProgressCard:
    def test_returns_card_with_steps(self):
        card = progress_card(
            instance_id="abc-123",
            steps=[{"step": 1, "name": "Planner", "status": "completed"},
                   {"step": 2, "name": "Developer", "status": "in_progress"}],
            story_id="TEST-001",
        )
        assert card["type"] == "AdaptiveCard"

    def test_handles_empty_steps(self):
        card = progress_card(instance_id="abc-123", steps=[])
        assert card is not None
        assert card["type"] == "AdaptiveCard"


class TestCompletionCard:
    def test_returns_card_with_results(self):
        card = completion_card(
            story_id="TEST-001", title="Sales Report",
            deployed=["bronze.Sales", "gold.vw_sales"],
            skipped=[], failed=[], elapsed=120,
        )
        assert card["type"] == "AdaptiveCard"
        body_str = str(card["body"])
        assert "TEST-001" in body_str or "Sales Report" in body_str

    def test_handles_failures(self):
        card = completion_card(
            story_id="TEST-001", title="Failed",
            deployed=[], skipped=[], failed=["gold.vw_bad"], elapsed=60,
        )
        assert card is not None
        assert card["type"] == "AdaptiveCard"
