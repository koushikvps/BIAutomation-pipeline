"""Healer Agent: Diagnoses and remediates failures from validation or deployment."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from shared.config import AppConfig
from shared.llm_client import LLMClient
from shared.models import (
    ArtifactBundle,
    GeneratedArtifact,
    HealerAction,
    HealerResult,
    ValidationReport,
    ValidationStatus,
)
from shared.synapse_client import SynapseClient

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).parent / "prompts"

MAX_HEAL_ATTEMPTS = 3

# Failure types and their auto-heal eligibility
FAILURE_CLASSIFICATION = {
    "sql_syntax": {"severity": "medium", "auto_healable": True},
    "naming_convention": {"severity": "low", "auto_healable": True},
    "json_syntax": {"severity": "medium", "auto_healable": True},
    "dependency": {"severity": "high", "auto_healable": False},
    "row_count": {"severity": "high", "auto_healable": True},
    "null_check": {"severity": "high", "auto_healable": True},
    "duplicate_check": {"severity": "high", "auto_healable": True},
    "reconciliation": {"severity": "high", "auto_healable": False},
    "permission_denied": {"severity": "critical", "auto_healable": False},
}


class HealerAgent:
    def __init__(self, config: AppConfig):
        self._llm = LLMClient(config)
        self._synapse = SynapseClient(config)
        self._heal_prompt = (PROMPTS_DIR / "heal_sql.txt").read_text()

    def run(
        self,
        validation_report: ValidationReport,
        artifact_bundle: ArtifactBundle,
        attempt_number: int = 1,
    ) -> tuple[ArtifactBundle, list[HealerAction]]:
        """Attempt to heal all failures in the validation report."""
        logger.info(
            "Healer Agent started for %s (attempt %d, %d failures)",
            validation_report.story_id,
            attempt_number,
            len(validation_report.blocking_failures),
        )

        if attempt_number > MAX_HEAL_ATTEMPTS:
            logger.warning("Max heal attempts exceeded — escalating all")
            actions = [
                HealerAction(
                    story_id=validation_report.story_id,
                    failure_type="max_retries_exceeded",
                    severity="critical",
                    auto_healable=False,
                    action_taken="escalated",
                    original_error=f"Failed after {MAX_HEAL_ATTEMPTS} attempts",
                    attempt_number=attempt_number,
                    result=HealerResult.ESCALATED,
                )
            ]
            return artifact_bundle, actions

        actions: list[HealerAction] = []
        corrected_artifacts = list(artifact_bundle.artifacts)

        for check in validation_report.checks:
            if check.status != ValidationStatus.FAIL:
                continue

            classification = FAILURE_CLASSIFICATION.get(
                check.check_type, {"severity": "high", "auto_healable": False}
            )

            if not classification["auto_healable"]:
                actions.append(HealerAction(
                    story_id=validation_report.story_id,
                    failure_type=check.check_type,
                    severity=classification["severity"],
                    auto_healable=False,
                    action_taken="escalated",
                    original_error=check.message,
                    attempt_number=attempt_number,
                    result=HealerResult.ESCALATED,
                ))
                continue

            # Find the corresponding artifact
            artifact = self._find_artifact(check.target_object, corrected_artifacts)
            if not artifact:
                actions.append(HealerAction(
                    story_id=validation_report.story_id,
                    failure_type=check.check_type,
                    severity=classification["severity"],
                    auto_healable=False,
                    action_taken="escalated",
                    original_error=f"Could not find artifact for {check.target_object}",
                    attempt_number=attempt_number,
                    result=HealerResult.ESCALATED,
                ))
                continue

            # Attempt LLM-based heal
            heal_result = self._heal_sql(artifact, check)

            if heal_result["action"] == "fixed":
                # Replace artifact content with corrected SQL
                idx = corrected_artifacts.index(artifact)
                corrected_artifacts[idx] = GeneratedArtifact(
                    step=artifact.step,
                    artifact_type=artifact.artifact_type,
                    object_name=artifact.object_name,
                    layer=artifact.layer,
                    file_name=artifact.file_name,
                    content=heal_result["corrected_sql"],
                )
                actions.append(HealerAction(
                    story_id=validation_report.story_id,
                    failure_type=check.check_type,
                    severity=classification["severity"],
                    auto_healable=True,
                    action_taken="regenerated_sql",
                    original_error=check.message,
                    fix_applied=heal_result["change_summary"],
                    attempt_number=attempt_number,
                    result=HealerResult.FIXED,
                ))
            else:
                actions.append(HealerAction(
                    story_id=validation_report.story_id,
                    failure_type=check.check_type,
                    severity=classification["severity"],
                    auto_healable=False,
                    action_taken="escalated",
                    original_error=check.message,
                    fix_applied=heal_result.get("diagnosis"),
                    attempt_number=attempt_number,
                    result=HealerResult.ESCALATED,
                ))

        corrected_bundle = ArtifactBundle(
            story_id=artifact_bundle.story_id,
            artifacts=corrected_artifacts,
        )

        logger.info(
            "Healer completed: %d fixed, %d escalated",
            sum(1 for a in actions if a.result == HealerResult.FIXED),
            sum(1 for a in actions if a.result == HealerResult.ESCALATED),
        )

        return corrected_bundle, actions

    def _heal_sql(self, artifact, check) -> dict:
        """Use LLM to diagnose and fix a SQL error."""
        catalog_context = ""
        try:
            schema = artifact.object_name.split(".")[0].replace("[", "").replace("]", "")
            tables = self._synapse.execute_query(
                f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}'"
            )
            catalog_context = json.dumps(tables, default=str)
        except Exception as e:
            logger.warning("Non-critical error fetching catalog context for healing: %s", e)

        user_prompt = json.dumps({
            "original_sql": artifact.content,
            "error_message": check.message,
            "object_name": artifact.object_name,
            "check_type": check.check_type,
            "catalog_context": catalog_context,
        }, indent=2)

        return self._llm.chat_json(
            system_prompt=self._heal_prompt,
            user_prompt=user_prompt,
        )

    def heal_from_review(
        self,
        review_result: dict,
        artifact_bundle: ArtifactBundle,
        attempt_number: int = 1,
    ) -> tuple[ArtifactBundle, list[HealerAction]]:
        """Heal artifacts based on Code Review Agent findings.

        Converts code review findings into fixes using LLM.
        """
        logger.info(
            "Healer Agent (code review mode) attempt %d, verdict=%s, findings=%d",
            attempt_number, review_result.get("overall_verdict"), review_result.get("total_findings", 0),
        )

        if attempt_number > MAX_HEAL_ATTEMPTS:
            logger.warning("Max code review heal attempts exceeded — escalating")
            actions = [
                HealerAction(
                    story_id=artifact_bundle.story_id,
                    failure_type="code_review_max_retries",
                    severity="critical",
                    auto_healable=False,
                    action_taken="escalated",
                    original_error=f"Code review still failing after {MAX_HEAL_ATTEMPTS} heal attempts",
                    attempt_number=attempt_number,
                    result=HealerResult.ESCALATED,
                )
            ]
            return artifact_bundle, actions

        actions: list[HealerAction] = []
        corrected_artifacts = list(artifact_bundle.artifacts)

        for rev in review_result.get("reviews", []):
            if rev.get("verdict") == "APPROVE":
                continue

            findings = rev.get("findings", [])
            if not findings:
                continue

            artifact_name = rev.get("artifact_name", "")
            artifact = self._find_artifact(artifact_name, corrected_artifacts)
            if not artifact:
                actions.append(HealerAction(
                    story_id=artifact_bundle.story_id,
                    failure_type="code_review",
                    severity="high",
                    auto_healable=False,
                    action_taken="escalated",
                    original_error=f"Could not find artifact: {artifact_name}",
                    attempt_number=attempt_number,
                    result=HealerResult.ESCALATED,
                ))
                continue

            # Build a combined fix prompt from all findings for this artifact
            findings_text = ""
            for f in findings:
                findings_text += f"\n- [{f.get('severity','info').upper()}][{f.get('category','')}] {f.get('issue','')}"
                if f.get("fix"):
                    findings_text += f"\n  Suggested fix: {f['fix']}"
                if f.get("line_hint"):
                    findings_text += f"\n  Near: {f['line_hint']}"

            heal_prompt = f"""Fix the following SQL artifact based on code review findings.

ARTIFACT: {artifact.object_name} ({artifact.layer.value} layer)
ORIGINAL SQL:
{artifact.content}

CODE REVIEW FINDINGS:{findings_text}

Return JSON:
{{
  "action": "fixed",
  "corrected_sql": "<the complete corrected SQL>",
  "change_summary": "brief description of all changes made"
}}

If a finding cannot be fixed automatically, still fix everything you can.
Return the COMPLETE SQL (not just changed parts)."""

            result = self._llm.chat_json(
                system_prompt=self._heal_prompt,
                user_prompt=heal_prompt,
            )

            if result.get("action") == "fixed" and result.get("corrected_sql"):
                idx = corrected_artifacts.index(artifact)
                corrected_artifacts[idx] = GeneratedArtifact(
                    step=artifact.step,
                    artifact_type=artifact.artifact_type,
                    object_name=artifact.object_name,
                    layer=artifact.layer,
                    file_name=artifact.file_name,
                    content=result["corrected_sql"],
                )
                actions.append(HealerAction(
                    story_id=artifact_bundle.story_id,
                    failure_type="code_review",
                    severity="medium",
                    auto_healable=True,
                    action_taken="regenerated_sql",
                    original_error=findings_text[:500],
                    fix_applied=result.get("change_summary", ""),
                    attempt_number=attempt_number,
                    result=HealerResult.FIXED,
                ))
            else:
                actions.append(HealerAction(
                    story_id=artifact_bundle.story_id,
                    failure_type="code_review",
                    severity="high",
                    auto_healable=False,
                    action_taken="escalated",
                    original_error=findings_text[:500],
                    attempt_number=attempt_number,
                    result=HealerResult.ESCALATED,
                ))

        corrected_bundle = ArtifactBundle(
            story_id=artifact_bundle.story_id,
            artifacts=corrected_artifacts,
        )

        fixed = sum(1 for a in actions if a.result == HealerResult.FIXED)
        escalated = sum(1 for a in actions if a.result == HealerResult.ESCALATED)
        logger.info("Code review heal: %d fixed, %d escalated", fixed, escalated)

        return corrected_bundle, actions

    @staticmethod
    def _find_artifact(target_object: str, artifacts: list[GeneratedArtifact]):
        """Find the artifact matching a validation target."""
        normalized = target_object.replace("[", "").replace("]", "").lower()
        for a in artifacts:
            a_name = a.object_name.replace("[", "").replace("]", "").lower()
            if normalized in a_name or a_name in normalized:
                return a
        return None
