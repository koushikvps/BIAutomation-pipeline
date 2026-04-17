"""Tests for Supervisor Agent."""

from unittest.mock import MagicMock, patch
import pytest
import time

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../agents"))

from supervisor.agent import SupervisorAgent, SupervisorVerdict, SLAConfig


class TestSupervisorCheckPlan:
    def _make_supervisor(self, llm_response):
        with patch("supervisor.agent.LLMClient") as MockLLM:
            instance = MockLLM.return_value
            instance.chat_json.return_value = llm_response
            config = MagicMock()
            return SupervisorAgent(config)

    def test_approves_valid_plan(self):
        sv = self._make_supervisor({
            "approved": True, "action": "continue", "reason": "Plan looks good",
            "quality_score": 0.9, "warnings": [],
        })
        verdict = sv.check_plan(
            plan={"steps": [{"agent": "planner"}, {"agent": "developer"}]},
            story={"description": "test"}, mode="greenfield")
        assert verdict.approved is True
        assert verdict.action == "continue"
        assert verdict.quality_score == 0.9

    def test_rejects_bad_plan(self):
        sv = self._make_supervisor({
            "approved": False, "action": "halt", "reason": "No human review gate",
            "quality_score": 0.2, "warnings": ["missing_human_review"],
        })
        verdict = sv.check_plan(plan={"steps": []}, story={}, mode="greenfield")
        assert verdict.approved is False
        assert verdict.action == "halt"

    def test_adds_missing_steps(self):
        sv = self._make_supervisor({
            "approved": False, "action": "override", "reason": "Missing validator",
            "quality_score": 0.6, "overrides": {"missing_steps": ["validator_post"]},
            "warnings": [],
        })
        verdict = sv.check_plan(plan={"steps": []}, story={}, mode="greenfield")
        assert verdict.overrides["missing_steps"] == ["validator_post"]

    def test_llm_failure_allows_plan(self):
        with patch("supervisor.agent.LLMClient") as MockLLM:
            instance = MockLLM.return_value
            instance.chat_json.side_effect = Exception("LLM down")
            config = MagicMock()
            sv = SupervisorAgent(config)
        verdict = sv.check_plan(plan={}, story={}, mode="greenfield")
        assert verdict.approved is True
        assert "Supervisor LLM unavailable" in verdict.warnings


class TestSupervisorCheckStep:
    def _make_supervisor(self, llm_response, sla=None):
        with patch("supervisor.agent.LLMClient") as MockLLM:
            instance = MockLLM.return_value
            instance.chat_json.return_value = llm_response
            config = MagicMock()
            return SupervisorAgent(config, sla=sla)

    def test_approves_good_result(self):
        sv = self._make_supervisor({
            "approved": True, "action": "continue", "reason": "Good",
            "quality_score": 0.85, "warnings": [],
        })
        verdict = sv.check_step_result("s1", "planner", {"plan": {}}, {"decision": "proceed"}, 5.0)
        assert verdict.approved is True

    def test_halts_on_sla_breach(self):
        sla = SLAConfig(max_duration_minutes=10)
        sv = self._make_supervisor({}, sla=sla)
        verdict = sv.check_step_result("s1", "planner", {}, {}, elapsed_minutes=15.0)
        assert verdict.approved is False
        assert verdict.action == "halt"
        assert "SLA" in verdict.reason

    def test_halts_on_retry_limit(self):
        sla = SLAConfig(max_total_retries=2)
        sv = self._make_supervisor({}, sla=sla)
        sv._total_retries = 3
        verdict = sv.check_step_result("s1", "planner", {}, {"decision": "proceed"}, 1.0)
        assert verdict.approved is False
        assert verdict.action == "halt"

    def test_trusts_commander_for_non_critical(self):
        sv = self._make_supervisor({})
        verdict = sv.check_step_result("s1", "notify_teams", {}, {"decision": "proceed", "quality_score": 0.8}, 1.0)
        assert verdict.approved is True
        assert "trusting Commander" in verdict.reason

    def test_tracks_retries(self):
        sv = self._make_supervisor({
            "approved": True, "action": "continue", "reason": "ok",
            "quality_score": 0.7, "warnings": [],
        })
        sv.check_step_result("s1", "planner", {}, {"decision": "retry"}, 1.0)
        assert sv._total_retries == 1


class TestSupervisorFinalSignoff:
    def _make_supervisor(self, llm_response):
        with patch("supervisor.agent.LLMClient") as MockLLM:
            instance = MockLLM.return_value
            instance.chat_json.return_value = llm_response
            config = MagicMock()
            return SupervisorAgent(config)

    def test_approves_successful_pipeline(self):
        sv = self._make_supervisor({
            "approved": True, "action": "continue", "reason": "All steps passed",
            "quality_score": 0.9, "warnings": [],
        })
        verdict = sv.final_signoff({"completed": 5, "failed": 0, "skipped": 0})
        assert verdict.approved is True

    def test_rejects_failed_pipeline(self):
        sv = self._make_supervisor({
            "approved": False, "action": "escalate", "reason": "Critical failures",
            "quality_score": 0.3, "warnings": ["deploy_failed"],
        })
        verdict = sv.final_signoff({"completed": 3, "failed": 2, "skipped": 0})
        assert verdict.approved is False

    def test_llm_failure_checks_failures(self):
        with patch("supervisor.agent.LLMClient") as MockLLM:
            instance = MockLLM.return_value
            instance.chat_json.side_effect = Exception("LLM down")
            config = MagicMock()
            sv = SupervisorAgent(config)
        verdict = sv.final_signoff({"completed": 3, "failed": 2})
        assert verdict.approved is False
        verdict2 = sv.final_signoff({"completed": 5, "failed": 0})
        assert verdict2.approved is True


class TestSupervisorVerdict:
    def test_to_dict(self):
        v = SupervisorVerdict(approved=True, action="continue", reason="ok", quality_score=0.8)
        d = v.to_dict()
        assert d["approved"] is True
        assert d["quality_score"] == 0.8
        assert isinstance(d["warnings"], list)


class TestSLAConfig:
    def test_defaults(self):
        sla = SLAConfig()
        assert sla.max_duration_minutes == 30
        assert sla.max_total_retries == 5
        assert sla.max_llm_calls == 50

    def test_custom_values(self):
        sla = SLAConfig(max_duration_minutes=10, max_total_retries=3)
        assert sla.max_duration_minutes == 10
        assert sla.max_total_retries == 3


class TestSupervisorAuditTrail:
    def test_audit_trail_structure(self):
        with patch("supervisor.agent.LLMClient") as MockLLM:
            instance = MockLLM.return_value
            instance.chat_json.return_value = {"approved": True, "action": "continue", "reason": "ok", "quality_score": 0.8, "warnings": []}
            sv = SupervisorAgent(MagicMock())
        sv.check_step_result("s1", "planner", {}, {}, 1.0)
        trail = sv.get_audit_trail()
        assert trail["total_checks"] == 1
        assert len(trail["step_verdicts"]) == 1
        assert "elapsed_minutes" in trail
