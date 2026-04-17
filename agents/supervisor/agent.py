"""Supervisor Agent — the watchdog.

Monitors the Commander's execution independently. Does NOT execute tasks —
only evaluates whether the Commander is making good decisions and the
pipeline is on track.

Responsibilities:
- SLA enforcement (duration, retries, cost)
- Output quality validation (not just "did it finish" but "is it correct")
- Commander override (force-stop runaway loops, reject bad plans)
- Final sign-off before reporting success to user
- Escalation to human if Commander itself fails
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from shared.config import AppConfig
from shared.llm_client import LLMClient

logger = logging.getLogger(__name__)


@dataclass
class SupervisorVerdict:
    approved: bool
    action: str  # "continue" | "override" | "halt" | "escalate"
    reason: str
    quality_score: float = 0.0
    overrides: dict = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "approved": self.approved,
            "action": self.action,
            "reason": self.reason,
            "quality_score": self.quality_score,
            "overrides": self.overrides,
            "warnings": self.warnings,
        }


@dataclass
class SLAConfig:
    max_duration_minutes: int = 30
    max_total_retries: int = 5
    max_llm_calls: int = 50
    max_cost_usd: float = 5.0
    required_quality_score: float = 0.7


class SupervisorAgent:
    """Independent watchdog that monitors Commander decisions and pipeline quality."""

    def __init__(self, config: AppConfig, sla: SLAConfig | None = None):
        self._llm = LLMClient(config)
        self.sla = sla or SLAConfig()
        self._start_time = time.time()
        self._llm_calls = 0
        self._total_retries = 0
        self._step_verdicts: list[dict] = []

    def check_plan(self, plan: dict, story: dict, mode: str) -> SupervisorVerdict:
        """Validate Commander's execution plan before it starts.

        Checks:
        - Are all required steps present (planner, human review, deploy, validate)?
        - Is the ordering logical (no deploy before code review)?
        - Is the plan appropriate for the mode?
        - Are there any missing agents?
        """
        system_prompt = """You are the Supervisor Agent reviewing the Commander's execution plan.
Your job is to validate the plan before execution begins.

Check for:
1. Required steps present: notify, plan, human_review, develop, code_review, deploy, validate
2. Logical ordering: no deployment before review, no SQL deploy before ADF
3. Mode appropriateness: integration mode should NOT have direct deployer_sql/deployer_adf
4. Security: human review gate must exist before any deployment
5. Completeness: notify_teams at start and end

Respond with JSON:
{
  "approved": true/false,
  "action": "continue" | "override" | "halt",
  "reason": "explanation",
  "quality_score": 0.0 to 1.0,
  "warnings": ["warning1", "warning2"],
  "overrides": {"missing_steps": ["step_to_add"], "reorder": []}
}"""

        plan_text = json.dumps(plan, default=str)[:3000]
        user_prompt = f"Mode: {mode}\nStory: {json.dumps(story, default=str)[:1000]}\nPlan:\n{plan_text}"

        try:
            result = self._llm.chat_json(system_prompt, user_prompt, max_tokens=800)
            self._llm_calls += 1
        except Exception as e:
            logger.warning("Supervisor plan check failed: %s", e)
            return SupervisorVerdict(
                approved=True, action="continue",
                reason=f"Supervisor check failed ({e}), allowing plan to proceed",
                quality_score=0.5, warnings=["Supervisor LLM unavailable"]
            )

        return SupervisorVerdict(
            approved=result.get("approved", True),
            action=result.get("action", "continue"),
            reason=result.get("reason", ""),
            quality_score=result.get("quality_score", 0.5),
            overrides=result.get("overrides", {}),
            warnings=result.get("warnings", []),
        )

    def check_step_result(self, step_name: str, agent: str, result: Any,
                          commander_decision: dict, elapsed_minutes: float) -> SupervisorVerdict:
        """Validate a single step's output and Commander's decision about it.

        This is the core watchdog function — called after every agent execution.
        """
        # SLA checks first (no LLM needed)
        sla_warnings = []
        if elapsed_minutes > self.sla.max_duration_minutes:
            return SupervisorVerdict(
                approved=False, action="halt",
                reason=f"SLA breached: {elapsed_minutes:.0f}min > {self.sla.max_duration_minutes}min limit",
                warnings=["SLA_DURATION_EXCEEDED"]
            )

        if self._total_retries > self.sla.max_total_retries:
            return SupervisorVerdict(
                approved=False, action="halt",
                reason=f"Too many retries: {self._total_retries} > {self.sla.max_total_retries} limit",
                warnings=["SLA_RETRIES_EXCEEDED"]
            )

        if self._llm_calls > self.sla.max_llm_calls:
            sla_warnings.append(f"LLM call budget: {self._llm_calls}/{self.sla.max_llm_calls}")

        if commander_decision.get("decision") in ("retry", "reroute"):
            self._total_retries += 1

        # LLM quality check for critical steps
        critical_agents = {"planner", "developer", "code_review", "validator_post", "deployer_sql"}
        if agent not in critical_agents:
            verdict = SupervisorVerdict(
                approved=True, action="continue",
                reason=f"Non-critical step '{agent}' — trusting Commander",
                quality_score=commander_decision.get("quality_score", 0.8),
                warnings=sla_warnings,
            )
            self._step_verdicts.append({"step": step_name, "verdict": verdict.to_dict()})
            return verdict

        result_summary = json.dumps(result, default=str)[:2000] if result else "null"
        cmd_summary = json.dumps(commander_decision, default=str)[:500]

        system_prompt = """You are the Supervisor Agent validating a critical pipeline step.
You are independent from the Commander — your job is to catch mistakes the Commander might miss.

Check:
1. Does the output make sense for this agent type?
2. Is the Commander's decision (proceed/retry/escalate) appropriate?
3. Are there quality red flags (empty output, placeholder values, SQL injection)?
4. Would you override the Commander's decision?

Respond with JSON:
{
  "approved": true/false,
  "action": "continue" | "override" | "escalate",
  "reason": "explanation",
  "quality_score": 0.0 to 1.0,
  "warnings": [],
  "overrides": {"force_retry": false, "force_halt": false, "override_reason": ""}
}"""

        user_prompt = (
            f"Agent: {agent}\nStep: {step_name}\n"
            f"Result summary: {result_summary}\n"
            f"Commander decision: {cmd_summary}\n"
            f"Elapsed: {elapsed_minutes:.1f}min"
        )

        try:
            check = self._llm.chat_json(system_prompt, user_prompt, max_tokens=500)
            self._llm_calls += 1
        except Exception as e:
            logger.warning("Supervisor step check failed: %s", e)
            return SupervisorVerdict(
                approved=True, action="continue",
                reason="Supervisor check unavailable, trusting Commander",
                quality_score=0.5, warnings=sla_warnings + ["supervisor_llm_error"],
            )

        verdict = SupervisorVerdict(
            approved=check.get("approved", True),
            action=check.get("action", "continue"),
            reason=check.get("reason", ""),
            quality_score=check.get("quality_score", 0.5),
            overrides=check.get("overrides", {}),
            warnings=sla_warnings + check.get("warnings", []),
        )
        self._step_verdicts.append({"step": step_name, "verdict": verdict.to_dict()})
        return verdict

    def final_signoff(self, execution_summary: dict) -> SupervisorVerdict:
        """Final quality gate — validates the entire pipeline execution before reporting success.

        This is the last check before the user is told "pipeline succeeded."
        """
        elapsed = (time.time() - self._start_time) / 60

        system_prompt = """You are the Supervisor Agent performing final sign-off on a completed pipeline.

This is the LAST gate before the user is told the pipeline succeeded.
Be thorough — check for:
1. All critical steps completed (plan, develop, review, deploy, validate)
2. No skipped critical steps
3. Quality scores across steps are acceptable (avg > 0.7)
4. Retry count is reasonable (< 5 total)
5. No unresolved warnings

Respond with JSON:
{
  "approved": true/false,
  "action": "continue" | "escalate",
  "reason": "final assessment",
  "quality_score": 0.0 to 1.0 (overall pipeline quality),
  "warnings": []
}"""

        summary_text = json.dumps(execution_summary, default=str)[:3000]
        verdicts_text = json.dumps(self._step_verdicts[-10:], default=str)[:1500]

        user_prompt = (
            f"Execution summary:\n{summary_text}\n\n"
            f"Step verdicts:\n{verdicts_text}\n\n"
            f"Total elapsed: {elapsed:.1f}min\n"
            f"Total LLM calls (supervisor): {self._llm_calls}\n"
            f"Total retries: {self._total_retries}"
        )

        try:
            result = self._llm.chat_json(system_prompt, user_prompt, max_tokens=500)
            self._llm_calls += 1
        except Exception as e:
            logger.warning("Supervisor final signoff failed: %s", e)
            has_failures = execution_summary.get("failed", 0) > 0
            return SupervisorVerdict(
                approved=not has_failures,
                action="continue" if not has_failures else "escalate",
                reason=f"Supervisor signoff unavailable. Failed steps: {execution_summary.get('failed', 0)}",
                quality_score=0.5,
                warnings=["supervisor_signoff_unavailable"],
            )

        return SupervisorVerdict(
            approved=result.get("approved", True),
            action=result.get("action", "continue"),
            reason=result.get("reason", ""),
            quality_score=result.get("quality_score", 0.5),
            warnings=result.get("warnings", []),
        )

    def get_audit_trail(self) -> dict:
        """Return complete supervisor audit trail for transparency."""
        return {
            "total_checks": len(self._step_verdicts),
            "llm_calls": self._llm_calls,
            "total_retries_tracked": self._total_retries,
            "elapsed_minutes": (time.time() - self._start_time) / 60,
            "step_verdicts": self._step_verdicts,
        }
