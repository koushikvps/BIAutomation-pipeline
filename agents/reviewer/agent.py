"""Code Review Agent: AI reviews generated SQL/ADF artifacts before deployment.

Checks for:
- SQL injection risks (dynamic SQL, unsanitized inputs)
- Performance issues (missing indexes, Cartesian joins, full table scans)
- Synapse-specific anti-patterns (unsupported types, bad distribution)
- Naming convention compliance
- Idempotency (safe to re-run)
- Business logic correctness (SQL matches the build plan)
- ADF pipeline structure validity
"""

from __future__ import annotations

import json
import logging

from shared.config import AppConfig
from shared.llm_client import LLMClient

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a senior data engineer performing a rigorous code review on AI-generated
SQL and ADF pipeline artifacts before they are deployed to Azure Synapse Analytics (Dedicated SQL Pool).

Review each artifact for the following categories:

1. SECURITY
   - SQL injection risks: dynamic SQL, EXEC with string concatenation, unsanitized parameters
   - Exposed credentials or secrets in code
   - Overly permissive grants (GRANT ALL, public schema writes)

2. PERFORMANCE
   - Missing WHERE clauses on large tables (full table scans)
   - Cartesian joins (missing JOIN conditions)
   - SELECT * instead of explicit columns
   - Missing DISTRIBUTION hints for Synapse (HASH, ROUND_ROBIN, REPLICATE)
   - Unnecessary DISTINCT or ORDER BY in subqueries
   - Correlated subqueries that could be JOINs

3. SYNAPSE COMPATIBILITY
   - Unsupported data types (VARCHAR(MAX) should be VARCHAR(8000))
   - Missing CLUSTERED COLUMNSTORE INDEX (Synapse best practice)
   - IDENTITY columns without proper handling
   - Temp tables vs CTEs in Synapse context
   - External table format issues (PARQUET, file path patterns)

4. NAMING & STANDARDS
   - Schema prefix matches layer (bronze.*, silver.*, gold.*)
   - Consistent naming convention (snake_case preferred)
   - Procedure names follow pattern: sp_<layer>_<entity>
   - View names follow pattern: vw_<entity>

5. IDEMPOTENCY
   - CREATE should use IF NOT EXISTS or DROP IF EXISTS first
   - INSERT should handle duplicates (MERGE or DELETE+INSERT)
   - Procedures should be CREATE OR ALTER

6. BUSINESS LOGIC
   - SQL actually implements what the build plan describes
   - JOIN conditions are correct (right columns, right tables)
   - Aggregations match requirements (SUM, COUNT, AVG on correct columns)
   - Filters match acceptance criteria

7. ADF PIPELINE (if present)
   - Valid JSON structure
   - Linked service references exist
   - Copy activity mappings are correct
   - Trigger schedule is reasonable

For each artifact, return:
{
  "artifact_name": "schema.object_name",
  "verdict": "APPROVE" | "NEEDS_FIX" | "REJECT",
  "severity": "info" | "warning" | "critical",
  "findings": [
    {
      "category": "security|performance|synapse|naming|idempotency|logic|adf",
      "severity": "info|warning|critical",
      "line_hint": "approximate location or SQL snippet",
      "issue": "clear description of the problem",
      "fix": "suggested fix or corrected SQL"
    }
  ],
  "summary": "one-line summary of review result"
}

Return a JSON object:
{
  "overall_verdict": "APPROVE" | "NEEDS_FIX" | "REJECT",
  "reviews": [ ... one per artifact ... ],
  "total_findings": 0,
  "critical_count": 0,
  "warning_count": 0,
  "info_count": 0,
  "review_summary": "Overall assessment in 1-2 sentences"
}

Rules:
- APPROVE: No critical or warning issues. Safe to deploy.
- NEEDS_FIX: Has warnings but can deploy with caution. List all fixes.
- REJECT: Has critical issues. Must fix before deploy.
- Be specific: quote the problematic SQL, suggest exact fixes.
- Don't flag style preferences as warnings (e.g., alias naming).
"""


class CodeReviewAgent:
    def __init__(self, config: AppConfig):
        self._llm = LLMClient(config)

    def review(self, artifacts: list[dict], build_plan: dict) -> dict:
        """Review all artifacts against the build plan."""
        logger.info("Code Review Agent: reviewing %d artifacts", len(artifacts))

        # Build the review prompt with all artifacts
        artifact_text = ""
        for i, art in enumerate(artifacts, 1):
            name = art.get("object_name", art.get("file_name", f"artifact_{i}"))
            layer = art.get("layer", "unknown")
            art_type = art.get("artifact_type", "unknown")
            content = art.get("content", "")

            artifact_text += f"\n--- ARTIFACT {i}: {layer}.{name} (type: {art_type}) ---\n"
            artifact_text += content
            artifact_text += "\n"

        # Build plan context
        plan_steps = build_plan.get("execution_order", [])
        plan_text = ""
        for step in plan_steps:
            if isinstance(step, dict):
                plan_text += f"  - {step.get('layer','')}.{step.get('object_name','')}: {step.get('action','')} ({step.get('logic_summary','')})\n"

        user_prompt = f"""Review these artifacts before deployment to Azure Synapse Dedicated SQL Pool.

BUILD PLAN (what was requested):
{plan_text}

ARTIFACTS TO REVIEW:
{artifact_text}

Perform a thorough code review covering all 7 categories. Be specific with findings.
"""

        result = self._llm.chat_json(
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user_prompt,
            temperature=0.0,
            max_tokens=4096,
        )

        # Ensure required fields
        result.setdefault("overall_verdict", "APPROVE")
        result.setdefault("reviews", [])
        result.setdefault("total_findings", 0)
        result.setdefault("critical_count", 0)
        result.setdefault("warning_count", 0)
        result.setdefault("info_count", 0)
        result.setdefault("review_summary", "")

        # Count findings
        total = 0
        critical = 0
        warnings = 0
        infos = 0
        for rev in result.get("reviews", []):
            for f in rev.get("findings", []):
                total += 1
                sev = f.get("severity", "info")
                if sev == "critical":
                    critical += 1
                elif sev == "warning":
                    warnings += 1
                else:
                    infos += 1

        result["total_findings"] = total
        result["critical_count"] = critical
        result["warning_count"] = warnings
        result["info_count"] = infos

        # Override verdict based on findings
        if critical > 0:
            result["overall_verdict"] = "REJECT"
        elif warnings > 0:
            result["overall_verdict"] = "NEEDS_FIX"

        logger.info(
            "Code Review: %s — %d findings (%d critical, %d warning, %d info)",
            result["overall_verdict"], total, critical, warnings, infos,
        )

        return result
