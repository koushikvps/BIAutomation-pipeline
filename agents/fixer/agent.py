"""Bug Fixer Agent: Reads an ADO bug, analyzes root cause, generates corrected code.

Closes the loop: Build -> Test -> Bug Found -> Auto-Fix -> Re-Test -> Green.

Fix types:
  - data_fix: Wrong SQL logic, missing join, bad filter -> regenerates corrected SQL
  - pipeline_fix: ADF config, source mapping -> regenerates pipeline JSON
  - ui_recommendation: Power App issues -> generates detailed fix recommendation
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from shared.config import AppConfig
from shared.llm_client import LLMClient
from shared.synapse_client import SynapseClient

logger = logging.getLogger(__name__)

FIX_SYSTEM_PROMPT = """You are a senior data engineer fixing bugs in Azure Synapse SQL and ADF pipelines.

You will receive:
1. The ADO Bug description (title, repro steps, expected vs actual behavior)
2. The original SQL/ADF artifact that caused the issue (if available)
3. The existing Synapse catalog context (schemas, tables, columns)

Your job:
- Diagnose the root cause from the bug description
- Generate the COMPLETE corrected SQL or ADF JSON (not just a diff)
- Explain what you changed and why

Return JSON:
{
    "fix_type": "data_fix" | "pipeline_fix" | "ui_recommendation",
    "root_cause": "One sentence explaining why the bug exists",
    "corrected_artifacts": [
        {
            "object_name": "[schema].[object]",
            "layer": "bronze" | "silver" | "gold",
            "artifact_type": "table" | "view" | "procedure" | "external_table" | "adf_pipeline",
            "file_name": "filename.sql",
            "content": "COMPLETE corrected SQL or JSON"
        }
    ],
    "change_summary": "Bullet list of all changes made",
    "confidence": "high" | "medium" | "low",
    "recommendation": "Additional notes for the reviewer (optional)"
}

RULES:
- Return the COMPLETE SQL, not partial. Include CREATE OR ALTER, full column list, etc.
- For data bugs, check that JOINs, WHERE filters, aggregations, and column mappings are correct.
- For pipeline bugs, ensure linked service refs, dataset mappings, and copy activities are correct.
- If the bug is a UI issue (Power App), set fix_type to "ui_recommendation" and describe the exact
  field, rule, formula, or control that needs to change in the recommendation field.
- If you cannot determine the fix with confidence, set confidence to "low" and explain in recommendation.
"""


class BugFixerAgent:
    """Reads ADO bug details, analyzes root cause, generates fix."""

    def __init__(self, config: AppConfig):
        self._config = config
        self._llm = LLMClient(config)
        self._synapse = SynapseClient(config)

    def analyze_and_fix(self, bug_details: dict, original_artifacts: list[dict] = None) -> dict:
        """Analyze a bug and generate corrected code.

        Args:
            bug_details: ADO bug fields (title, description, repro_steps, etc.)
            original_artifacts: List of original artifacts that may need fixing
                Each: {object_name, layer, artifact_type, content, file_name}

        Returns:
            Fix result dict with corrected_artifacts, root_cause, etc.
        """
        bug_id = bug_details.get("id", "unknown")
        title = bug_details.get("title", "")
        logger.info("Bug Fixer analyzing bug #%s: %s", bug_id, title)

        # Gather context from Synapse catalog
        catalog_context = self._get_catalog_context(bug_details, original_artifacts)

        # Build the prompt
        user_prompt = json.dumps({
            "bug": {
                "id": bug_id,
                "title": title,
                "description": bug_details.get("description", ""),
                "repro_steps": bug_details.get("repro_steps", ""),
                "expected": bug_details.get("expected", ""),
                "actual": bug_details.get("actual", ""),
                "severity": bug_details.get("severity", ""),
                "test_name": bug_details.get("test_name", ""),
                "error_message": bug_details.get("error_message", ""),
            },
            "original_artifacts": original_artifacts or [],
            "catalog_context": catalog_context,
        }, indent=2, default=str)

        fix_result = self._llm.chat_json(
            system_prompt=FIX_SYSTEM_PROMPT,
            user_prompt=user_prompt,
        )

        fix_type = fix_result.get("fix_type", "unknown")
        confidence = fix_result.get("confidence", "low")
        artifact_count = len(fix_result.get("corrected_artifacts", []))

        logger.info("Bug #%s: fix_type=%s, confidence=%s, artifacts=%d",
                     bug_id, fix_type, confidence, artifact_count)

        return {
            "bug_id": bug_id,
            "bug_title": title,
            "fix_type": fix_type,
            "root_cause": fix_result.get("root_cause", ""),
            "corrected_artifacts": fix_result.get("corrected_artifacts", []),
            "change_summary": fix_result.get("change_summary", ""),
            "confidence": confidence,
            "recommendation": fix_result.get("recommendation", ""),
        }

    def _get_catalog_context(self, bug_details: dict, artifacts: list[dict] = None) -> dict:
        """Gather Synapse catalog context relevant to the bug."""
        context = {"schemas": {}, "related_objects": []}

        # Extract object names from bug description and artifacts
        mentioned_objects = set()
        for text in [bug_details.get("title", ""), bug_details.get("description", ""),
                     bug_details.get("error_message", "")]:
            # Look for [schema].[object] patterns
            import re
            matches = re.findall(r'\[?(\w+)\]?\.\[?(\w+)\]?', text)
            for schema, obj in matches:
                mentioned_objects.add((schema, obj))

        if artifacts:
            for art in artifacts:
                name = art.get("object_name", "")
                parts = name.replace("[", "").replace("]", "").split(".")
                if len(parts) == 2:
                    mentioned_objects.add((parts[0], parts[1]))

        # Get columns for mentioned objects
        for schema, obj in mentioned_objects:
            try:
                cols = self._synapse.get_columns(schema, obj)
                context["related_objects"].append({
                    "schema": schema,
                    "object": obj,
                    "columns": [{"name": c["COLUMN_NAME"], "type": c["DATA_TYPE"]} for c in cols],
                })
            except Exception as e:
                logger.warning("Non-critical error fetching related object columns: %s", e)

        return context
