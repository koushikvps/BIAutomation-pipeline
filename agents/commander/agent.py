"""Commander Agent — the brain of the pipeline.

Receives a task, decomposes it into sub-tasks, dispatches to worker agents,
evaluates their output, and decides next actions dynamically.

Unlike the hardcoded orchestrator chain, the Commander reasons about:
- Which agents to invoke and in what order
- Whether to parallelize or serialize
- How to handle failures (retry same agent, try different agent, escalate)
- When to request human review
- What context to pass between agents
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

AVAILABLE_AGENTS = {
    "planner": "Creates an execution plan from a business requirement story. Returns structured BuildPlan with table mappings, SQL artifacts, and ADF pipeline specs.",
    "developer": "Generates SQL DDL (external tables, views) and ADF pipeline JSON from a BuildPlan. Returns ArtifactBundle.",
    "code_review": "Reviews generated SQL and ADF code for quality, security, naming conventions. Returns pass/fail with issues list.",
    "deployer_adf": "Deploys ADF pipeline and trigger to Azure Data Factory via REST API.",
    "deployer_sql": "Executes SQL DDL on Synapse dedicated pool to create/alter database objects.",
    "validator_pre": "Runs pre-deployment checks: naming conventions, SQL syntax, ADF JSON structure, dependency analysis.",
    "validator_post": "Runs post-deployment checks: verifies objects exist, row counts, data quality.",
    "healer": "Analyzes validation failures and generates corrected SQL/ADF code.",
    "bug_fixer": "Reads an ADO bug work item, analyzes root cause, generates fix.",
    "discovery": "Scans existing Synapse environment: schemas, tables, views, procs, naming patterns.",
    "convention_adapter": "Detects naming conventions from discovery profile, generates ConventionRuleset.",
    "pr_delivery": "Pushes generated artifacts to ADO Git repo as a Pull Request.",
    "notify_teams": "Sends Teams notification (started, review gate, progress, completion).",
}


@dataclass
class TaskStep:
    id: str
    agent: str
    description: str
    depends_on: list[str] = field(default_factory=list)
    parallel_with: list[str] = field(default_factory=list)
    requires_human_review: bool = False
    max_retries: int = 2
    status: str = "pending"
    result: Any = None
    attempts: int = 0
    feedback: str = ""


@dataclass
class ExecutionPlan:
    task_id: str
    story_id: str
    mode: str
    steps: list[TaskStep] = field(default_factory=list)
    sla_minutes: int = 30
    max_total_retries: int = 5
    total_retries_used: int = 0


class CommanderAgent:
    """Decomposes tasks, dispatches agents, evaluates results, decides next action."""

    def __init__(self, config: AppConfig):
        self._llm = LLMClient(config)

    def plan_execution(self, story: dict, mode: str = "greenfield",
                       catalog_context: str = "", convention_ruleset: dict | None = None) -> ExecutionPlan:
        """Decompose a story into an ordered execution plan."""
        story_text = story.get("description", "") or story.get("title", "")
        story_id = story.get("story_id", "unknown")

        agents_desc = "\n".join(f"- {k}: {v}" for k, v in AVAILABLE_AGENTS.items())

        system_prompt = f"""You are the Commander Agent for a BI data pipeline automation platform.
Your job is to decompose a business requirement into an execution plan — a sequence of agent tasks.

Available agents:
{agents_desc}

Deployment mode: {mode}
{"Convention ruleset is provided — use convention_adapter and pr_delivery instead of direct deploy." if convention_ruleset else ""}

Rules:
- Always start with notify_teams (pipeline started)
- Planner must run before Developer
- Code Review must run after Developer, before any deployment
- Human review is REQUIRED after Planner (the plan) and optionally after Code Review
- For integration mode: use discovery → convention_adapter → planner → developer → code_review → pr_delivery
- For greenfield/brownfield: planner → developer → code_review → deployer_adf → validator_pre → deployer_sql → validator_post
- Healer is NOT pre-planned — Commander invokes it dynamically on failures
- End with notify_teams (completion)

Return JSON only:
{{
  "steps": [
    {{
      "id": "s1",
      "agent": "agent_name",
      "description": "what this step does",
      "depends_on": ["s0"],
      "requires_human_review": false
    }}
  ],
  "sla_minutes": 30
}}"""

        user_prompt = f"Story: {story_text}\nMode: {mode}\nStory ID: {story_id}"
        if catalog_context:
            user_prompt += f"\nCatalog context: {catalog_context[:2000]}"

        result = self._llm.chat_json(system_prompt, user_prompt, max_tokens=2000)

        steps = []
        for s in result.get("steps", []):
            steps.append(TaskStep(
                id=s.get("id", f"s{len(steps)}"),
                agent=s.get("agent", ""),
                description=s.get("description", ""),
                depends_on=s.get("depends_on", []),
                parallel_with=s.get("parallel_with", []),
                requires_human_review=s.get("requires_human_review", False),
            ))

        return ExecutionPlan(
            task_id=f"cmd-{story_id}-{int(time.time())}",
            story_id=story_id,
            mode=mode,
            steps=steps,
            sla_minutes=result.get("sla_minutes", 30),
        )

    def evaluate_result(self, step: TaskStep, result: Any, context: dict) -> dict:
        """Evaluate an agent's output and decide next action.

        Returns:
            {"decision": "proceed"|"retry"|"reroute"|"escalate",
             "reason": str,
             "feedback": str,  # feedback for retry
             "reroute_to": str}  # agent name if rerouting
        """
        result_summary = json.dumps(result, default=str)[:3000] if result else "null"

        system_prompt = """You are the Commander Agent evaluating a worker agent's output.
Decide the next action based on the result quality.

Respond with JSON only:
{
  "decision": "proceed" | "retry" | "reroute" | "escalate",
  "reason": "brief explanation",
  "feedback": "specific feedback if retry (what to fix)",
  "reroute_to": "agent_name if rerouting, else null",
  "quality_score": 0.0 to 1.0
}

Rules:
- proceed: output is good, move to next step
- retry: output has fixable issues, send back to same agent with feedback (max 2 retries)
- reroute: this agent can't handle it, try a different agent
- escalate: critical failure, needs human intervention"""

        user_prompt = (
            f"Agent: {step.agent}\n"
            f"Task: {step.description}\n"
            f"Attempt: {step.attempts}/{step.max_retries}\n"
            f"Result: {result_summary}\n"
            f"Pipeline mode: {context.get('mode', 'greenfield')}"
        )

        try:
            evaluation = self._llm.chat_json(system_prompt, user_prompt, max_tokens=500)
        except Exception:
            return {"decision": "proceed", "reason": "LLM evaluation failed, proceeding", "quality_score": 0.5}

        if step.attempts >= step.max_retries and evaluation.get("decision") == "retry":
            evaluation["decision"] = "escalate"
            evaluation["reason"] = f"Max retries ({step.max_retries}) reached. {evaluation.get('reason', '')}"

        return evaluation

    def handle_failure(self, step: TaskStep, error: str, plan: ExecutionPlan) -> dict:
        """Decide how to handle an agent failure.

        Returns:
            {"action": "retry"|"heal"|"skip"|"abort",
             "reason": str,
             "healer_context": dict}
        """
        system_prompt = """You are the Commander Agent handling a failure.
An agent threw an error during execution. Decide recovery action.

Respond with JSON:
{
  "action": "retry" | "heal" | "skip" | "abort",
  "reason": "explanation",
  "healer_context": {"failure_type": "...", "suggested_fix": "..."}
}

Rules:
- retry: transient error (timeout, rate limit), try again
- heal: code/SQL error, invoke Healer agent with context
- skip: non-critical step that can be skipped (e.g., notify_teams)
- abort: unrecoverable (auth failure, missing resource), stop pipeline"""

        user_prompt = (
            f"Failed agent: {step.agent}\n"
            f"Task: {step.description}\n"
            f"Error: {error[:1000]}\n"
            f"Attempt: {step.attempts}/{step.max_retries}\n"
            f"Total pipeline retries used: {plan.total_retries_used}/{plan.max_total_retries}"
        )

        try:
            decision = self._llm.chat_json(system_prompt, user_prompt, max_tokens=500)
        except Exception:
            if "timeout" in error.lower() or "rate" in error.lower():
                return {"action": "retry", "reason": "Transient error detected"}
            return {"action": "abort", "reason": f"Cannot evaluate failure: {error[:200]}"}

        if plan.total_retries_used >= plan.max_total_retries and decision.get("action") in ("retry", "heal"):
            decision["action"] = "abort"
            decision["reason"] = f"Max total retries ({plan.max_total_retries}) exhausted"

        return decision

    def generate_summary(self, plan: ExecutionPlan) -> dict:
        """Generate final execution summary."""
        completed = [s for s in plan.steps if s.status == "completed"]
        failed = [s for s in plan.steps if s.status == "failed"]
        skipped = [s for s in plan.steps if s.status == "skipped"]

        return {
            "task_id": plan.task_id,
            "story_id": plan.story_id,
            "mode": plan.mode,
            "status": "success" if not failed else "partial" if completed else "failed",
            "total_steps": len(plan.steps),
            "completed": len(completed),
            "failed": len(failed),
            "skipped": len(skipped),
            "retries_used": plan.total_retries_used,
            "steps": [
                {
                    "id": s.id, "agent": s.agent, "status": s.status,
                    "attempts": s.attempts, "description": s.description,
                }
                for s in plan.steps
            ],
        }
