"""Tests for Commander Agent."""

from unittest.mock import MagicMock, patch
import pytest

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../agents"))

from commander.agent import CommanderAgent, TaskStep, ExecutionPlan, AVAILABLE_AGENTS


class TestCommanderPlanExecution:
    def _make_commander(self, llm_response):
        with patch("commander.agent.LLMClient") as MockLLM:
            instance = MockLLM.return_value
            instance.chat_json.return_value = llm_response
            config = MagicMock()
            return CommanderAgent(config)

    def test_plan_returns_execution_plan(self):
        cmd = self._make_commander({
            "steps": [
                {"id": "s1", "agent": "planner", "description": "Create plan", "depends_on": [], "requires_human_review": False},
                {"id": "s2", "agent": "developer", "description": "Generate SQL", "depends_on": ["s1"]},
            ],
            "sla_minutes": 20,
        })
        plan = cmd.plan_execution({"story_id": "TEST-1", "description": "Build sales pipeline"}, mode="greenfield")
        assert isinstance(plan, ExecutionPlan)
        assert plan.story_id == "TEST-1"
        assert plan.mode == "greenfield"
        assert len(plan.steps) == 2
        assert plan.steps[0].agent == "planner"
        assert plan.steps[1].agent == "developer"
        assert plan.sla_minutes == 20

    def test_plan_handles_empty_response(self):
        cmd = self._make_commander({"steps": [], "sla_minutes": 30})
        plan = cmd.plan_execution({"story_id": "X"})
        assert len(plan.steps) == 0

    def test_plan_sets_human_review_flag(self):
        cmd = self._make_commander({
            "steps": [{"id": "s1", "agent": "planner", "description": "plan", "requires_human_review": True}],
        })
        plan = cmd.plan_execution({"story_id": "X"})
        assert plan.steps[0].requires_human_review is True


class TestCommanderEvaluateResult:
    def _make_commander(self, eval_response):
        with patch("commander.agent.LLMClient") as MockLLM:
            instance = MockLLM.return_value
            instance.chat_json.return_value = eval_response
            config = MagicMock()
            return CommanderAgent(config)

    def test_proceed_decision(self):
        cmd = self._make_commander({"decision": "proceed", "reason": "Good output", "quality_score": 0.9})
        step = TaskStep(id="s1", agent="planner", description="test", attempts=1)
        result = cmd.evaluate_result(step, {"plan": {"tables": []}}, {"mode": "greenfield"})
        assert result["decision"] == "proceed"
        assert result["quality_score"] == 0.9

    def test_retry_escalates_after_max_retries(self):
        cmd = self._make_commander({"decision": "retry", "reason": "Bad output", "quality_score": 0.3})
        step = TaskStep(id="s1", agent="planner", description="test", attempts=3, max_retries=2)
        result = cmd.evaluate_result(step, {}, {})
        assert result["decision"] == "escalate"

    def test_llm_failure_defaults_to_proceed(self):
        with patch("commander.agent.LLMClient") as MockLLM:
            instance = MockLLM.return_value
            instance.chat_json.side_effect = Exception("LLM down")
            config = MagicMock()
            cmd = CommanderAgent(config)
        step = TaskStep(id="s1", agent="planner", description="test")
        result = cmd.evaluate_result(step, {}, {})
        assert result["decision"] == "proceed"


class TestCommanderHandleFailure:
    def _make_commander(self, failure_response):
        with patch("commander.agent.LLMClient") as MockLLM:
            instance = MockLLM.return_value
            instance.chat_json.return_value = failure_response
            config = MagicMock()
            return CommanderAgent(config)

    def test_retry_on_transient_error(self):
        cmd = self._make_commander({"action": "retry", "reason": "Transient timeout"})
        step = TaskStep(id="s1", agent="deployer_sql", description="deploy", attempts=1)
        plan = ExecutionPlan(task_id="t1", story_id="X", mode="greenfield", total_retries_used=1)
        result = cmd.handle_failure(step, "Connection timeout", plan)
        assert result["action"] == "retry"

    def test_abort_when_retries_exhausted(self):
        cmd = self._make_commander({"action": "retry", "reason": "Try again"})
        step = TaskStep(id="s1", agent="deployer_sql", description="deploy", attempts=3)
        plan = ExecutionPlan(task_id="t1", story_id="X", mode="greenfield", total_retries_used=5, max_total_retries=5)
        result = cmd.handle_failure(step, "Error", plan)
        assert result["action"] == "abort"

    def test_skip_non_critical(self):
        cmd = self._make_commander({"action": "skip", "reason": "Non-critical notification"})
        step = TaskStep(id="s1", agent="notify_teams", description="notify")
        plan = ExecutionPlan(task_id="t1", story_id="X", mode="greenfield")
        result = cmd.handle_failure(step, "Webhook down", plan)
        assert result["action"] == "skip"


class TestCommanderSummary:
    def test_summary_all_completed(self):
        with patch("commander.agent.LLMClient"):
            cmd = CommanderAgent(MagicMock())
        plan = ExecutionPlan(task_id="t1", story_id="S1", mode="greenfield", steps=[
            TaskStep(id="s1", agent="planner", description="plan", status="completed", attempts=1),
            TaskStep(id="s2", agent="developer", description="build", status="completed", attempts=1),
        ])
        summary = cmd.generate_summary(plan)
        assert summary["status"] == "success"
        assert summary["completed"] == 2
        assert summary["failed"] == 0

    def test_summary_with_failures(self):
        with patch("commander.agent.LLMClient"):
            cmd = CommanderAgent(MagicMock())
        plan = ExecutionPlan(task_id="t1", story_id="S1", mode="greenfield", steps=[
            TaskStep(id="s1", agent="planner", description="plan", status="completed"),
            TaskStep(id="s2", agent="developer", description="build", status="failed"),
        ])
        summary = cmd.generate_summary(plan)
        assert summary["status"] == "partial"
        assert summary["failed"] == 1


class TestAvailableAgents:
    def test_all_agents_documented(self):
        assert "planner" in AVAILABLE_AGENTS
        assert "developer" in AVAILABLE_AGENTS
        assert "healer" in AVAILABLE_AGENTS
        assert "discovery" in AVAILABLE_AGENTS
        assert "pr_delivery" in AVAILABLE_AGENTS
        assert len(AVAILABLE_AGENTS) >= 10
