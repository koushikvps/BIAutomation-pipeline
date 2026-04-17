"""Validator Agent: Pre-deploy static checks and post-deploy data quality checks."""

from __future__ import annotations

import logging
import re

from shared.config import AppConfig
from shared.models import (
    ArtifactBundle,
    ArtifactType,
    BuildPlan,
    ValidationCheck,
    ValidationReport,
    ValidationStatus,
)
from shared.synapse_client import SynapseClient

logger = logging.getLogger(__name__)

# Synapse dedicated pool incompatible patterns
SYNAPSE_BLOCKLIST = [
    (r"\bUSE\s+\w+", "USE database statements not supported"),
    (r"SELECT\s+\*\s+FROM", "SELECT * is not allowed — explicit column list required"),
    (r"\bCURSOR\b", "CURSOR is not recommended in Synapse"),
]

NAMING_PATTERNS = {
    "bronze": r"^\[?bronze\]?\.\[?\w+\]?$",
    "silver": r"^\[?silver\]?\.\[?(vw_|usp_load_)?\w+\]?$",
    "gold": r"^\[?gold\]?\.\[?vw_\w+\]?$",
}


class ValidatorAgent:
    def __init__(self, config: AppConfig):
        self._synapse = SynapseClient(config)

    def pre_deploy_check(self, bundle: ArtifactBundle, plan: BuildPlan) -> ValidationReport:
        """Static validation before deploying artifacts."""
        logger.info("Validator Agent: pre-deploy check for %s", bundle.story_id)
        checks: list[ValidationCheck] = []

        for artifact in bundle.artifacts:
            if artifact.artifact_type == ArtifactType.ADF_PIPELINE:
                checks.append(self._check_adf_json(artifact))
            else:
                checks.extend(self._check_sql_syntax(artifact))
                checks.append(self._check_naming(artifact))

        checks.extend(self._check_dependencies(bundle, plan))

        return self._build_report(bundle.story_id, "pre_deploy", checks)

    def post_deploy_check(self, plan: BuildPlan, environment: str) -> ValidationReport:
        """Runtime data quality validation after deployment."""
        logger.info("Validator Agent: post-deploy check for %s in %s", plan.story_id, environment)
        checks: list[ValidationCheck] = []

        for req in plan.validation_requirements:
            try:
                if req.check_type == "row_count":
                    checks.append(self._check_row_count(req, plan))
                elif req.check_type == "null_check":
                    checks.append(self._check_nulls(req))
                elif req.check_type == "duplicate_check":
                    checks.append(self._check_duplicates(req))
                elif req.check_type == "reconciliation":
                    checks.append(self._check_reconciliation(req))
            except Exception as e:
                checks.append(ValidationCheck(
                    check_name=f"{req.check_type}_error",
                    check_type=req.check_type,
                    layer=req.layer.value if req.layer else "unknown",
                    target_object=req.table or "unknown",
                    status=ValidationStatus.FAIL,
                    message=str(e),
                ))

        return self._build_report(plan.story_id, "post_deploy", checks)

    def _check_sql_syntax(self, artifact) -> list[ValidationCheck]:
        """Check for Synapse-incompatible SQL patterns."""
        checks = []
        content = re.sub(r'^```\w*\n?', '', artifact.content, flags=re.MULTILINE)
        content = re.sub(r'\n?```$', '', content, flags=re.MULTILINE)
        content = content.strip()
        for pattern, message in SYNAPSE_BLOCKLIST:
            match = re.search(pattern, content, re.IGNORECASE)
            status = ValidationStatus.FAIL if match else ValidationStatus.PASS
            checks.append(ValidationCheck(
                check_name=f"syntax_{pattern[:20]}",
                check_type="sql_syntax",
                layer=artifact.layer.value,
                target_object=artifact.object_name,
                status=status,
                message=message if match else "OK",
            ))
        return checks

    def _check_naming(self, artifact) -> ValidationCheck:
        """Verify object name matches naming convention."""
        layer = artifact.layer.value
        pattern = NAMING_PATTERNS.get(layer, r".*")
        match = re.match(pattern, artifact.object_name, re.IGNORECASE)
        return ValidationCheck(
            check_name=f"naming_{layer}",
            check_type="naming_convention",
            layer=layer,
            target_object=artifact.object_name,
            status=ValidationStatus.PASS if match else ValidationStatus.FAIL,
            message="OK" if match else f"Name does not match pattern: {pattern}",
        )

    def _check_adf_json(self, artifact) -> ValidationCheck:
        """Basic ADF JSON structure validation."""
        import json
        try:
            json.loads(artifact.content)
            return ValidationCheck(
                check_name="adf_json_valid",
                check_type="json_syntax",
                layer=artifact.layer.value,
                target_object=artifact.object_name,
                status=ValidationStatus.PASS,
                message="Valid JSON",
            )
        except json.JSONDecodeError as e:
            return ValidationCheck(
                check_name="adf_json_valid",
                check_type="json_syntax",
                layer=artifact.layer.value,
                target_object=artifact.object_name,
                status=ValidationStatus.FAIL,
                message=f"Invalid JSON: {e}",
            )

    def _check_dependencies(self, bundle: ArtifactBundle, plan: BuildPlan) -> list[ValidationCheck]:
        """Verify all referenced objects exist or are created in earlier steps."""
        checks = []
        created_objects = set()
        for step in plan.execution_order:
            name = step.object_name.replace("[", "").replace("]", "").lower()
            for dep_step_num in step.depends_on:
                dep_step = next((s for s in plan.execution_order if s.step == dep_step_num), None)
                if dep_step:
                    dep_name = dep_step.object_name.replace("[", "").replace("]", "").lower()
                    dep_exists = dep_name in created_objects
                    checks.append(ValidationCheck(
                        check_name=f"dependency_{step.step}_on_{dep_step_num}",
                        check_type="dependency",
                        layer=step.layer.value,
                        target_object=step.object_name,
                        status=ValidationStatus.PASS if dep_exists else ValidationStatus.WARN,
                        message=f"Depends on step {dep_step_num} ({dep_step.object_name})",
                    ))
            created_objects.add(name)
        return checks

    def _check_row_count(self, req, plan: BuildPlan) -> ValidationCheck:
        """Compare row counts between layers."""
        table_ref = req.table or ""
        if "." in table_ref:
            source_sql = f"SELECT COUNT(*) AS cnt FROM {table_ref}"
            target_sql = f"SELECT COUNT(*) AS cnt FROM {table_ref}"
        else:
            source_sql = f"SELECT COUNT(*) AS cnt FROM [{req.source_layer}].[{table_ref}]" if req.source_layer else req.source_query
            target_sql = f"SELECT COUNT(*) AS cnt FROM [{req.target_layer}].[{table_ref}]" if req.target_layer else req.target_query

        if req.source_query:
            source_sql = req.source_query
        if req.target_query:
            target_sql = req.target_query

        source_row = self._synapse.execute_query(source_sql)[0]
        source_count = list(source_row.values())[0]
        target_row = self._synapse.execute_query(target_sql)[0]
        target_count = list(target_row.values())[0]

        # Allow 5% tolerance
        diff_pct = abs(source_count - target_count) / max(source_count, 1) * 100
        status = ValidationStatus.PASS if diff_pct <= 5 else ValidationStatus.FAIL

        return ValidationCheck(
            check_name=f"row_count_{req.table}",
            check_type="row_count",
            layer=req.target_layer or "unknown",
            target_object=req.table or "unknown",
            expected_value=str(source_count),
            actual_value=str(target_count),
            status=status,
            query_executed=target_sql,
            message=f"Diff: {diff_pct:.1f}%",
        )

    def _check_nulls(self, req) -> ValidationCheck:
        """Check for unexpected NULLs in specified columns."""
        table_ref = req.table or ""
        if not table_ref.startswith("[") and "." in table_ref:
            schema, tbl = table_ref.split(".", 1)
            table_ref = f"[{schema}].[{tbl}]"

        existing_cols = []
        try:
            schema, tbl = req.table.split(".", 1) if "." in req.table else ("dbo", req.table)
            col_info = self._synapse.execute_query(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                params=(schema, tbl)
            )
            existing_cols = [r["COLUMN_NAME"].lower() for r in col_info]
        except Exception as e:
            logger.warning("Non-critical error fetching columns for null check on %s.%s: %s", schema, tbl, e)
            existing_cols = []

        valid_cols = [c for c in req.columns if not existing_cols or c.lower() in existing_cols]
        if not valid_cols:
            return ValidationCheck(
                check_name=f"null_check_{req.table}",
                check_type="null_check",
                layer=req.layer.value if req.layer else "unknown",
                target_object=req.table or "unknown",
                status=ValidationStatus.PASS,
                message="Skipped: referenced columns not found in table",
            )

        col_checks = " OR ".join(f"[{col}] IS NULL" for col in valid_cols)
        sql = f"SELECT COUNT(*) AS cnt FROM {table_ref} WHERE {col_checks}"
        result = self._synapse.execute_query(sql)[0]["cnt"]
        return ValidationCheck(
            check_name=f"null_check_{req.table}",
            check_type="null_check",
            layer=req.layer.value if req.layer else "unknown",
            target_object=req.table or "unknown",
            expected_value="0",
            actual_value=str(result),
            status=ValidationStatus.PASS if result == 0 else ValidationStatus.FAIL,
            query_executed=sql,
        )

    def _check_duplicates(self, req) -> ValidationCheck:
        """Check for duplicate keys."""
        table_ref = req.table or ""
        if not table_ref.startswith("[") and "." in table_ref:
            schema, tbl = table_ref.split(".", 1)
            table_ref = f"[{schema}].[{tbl}]"

        existing_cols = []
        try:
            schema, tbl = req.table.split(".", 1) if "." in req.table else ("dbo", req.table)
            col_info = self._synapse.execute_query(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                params=(schema, tbl)
            )
            existing_cols = [r["COLUMN_NAME"].lower() for r in col_info]
        except Exception as e:
            logger.warning("Non-critical error fetching columns for duplicate check on %s.%s: %s", schema, tbl, e)
            existing_cols = []

        valid_cols = [c for c in req.columns if not existing_cols or c.lower() in existing_cols]
        if not valid_cols:
            return ValidationCheck(
                check_name=f"duplicate_check_{req.table}",
                check_type="duplicate_check",
                layer=req.layer.value if req.layer else "unknown",
                target_object=req.table or "unknown",
                status=ValidationStatus.PASS,
                message="Skipped: referenced columns not found in table",
            )

        key_cols = ", ".join(f"[{col}]" for col in valid_cols)
        sql = f"SELECT {key_cols}, COUNT(*) AS cnt FROM {table_ref} GROUP BY {key_cols} HAVING COUNT(*) > 1"
        results = self._synapse.execute_query(sql)
        return ValidationCheck(
            check_name=f"duplicate_check_{req.table}",
            check_type="duplicate_check",
            layer=req.layer.value if req.layer else "unknown",
            target_object=req.table or "unknown",
            expected_value="0",
            actual_value=str(len(results)),
            status=ValidationStatus.PASS if len(results) == 0 else ValidationStatus.FAIL,
            query_executed=sql,
        )

    def _check_reconciliation(self, req) -> ValidationCheck:
        """Reconcile a metric between source and target."""
        source_val = self._synapse.execute_query(req.source_query)[0]
        target_val = self._synapse.execute_query(req.target_query)[0]
        s = list(source_val.values())[0]
        t = list(target_val.values())[0]
        if s is None or t is None:
            return ValidationCheck(
                check_name=f"reconciliation_{req.metric}",
                check_type="reconciliation",
                layer="gold",
                target_object=req.metric or "unknown",
                expected_value=str(s),
                actual_value=str(t),
                status=ValidationStatus.FAIL,
                message="NULL value returned from reconciliation query",
            )
        diff_pct = abs(float(s) - float(t)) / max(float(s), 0.01) * 100
        return ValidationCheck(
            check_name=f"reconciliation_{req.metric}",
            check_type="reconciliation",
            layer="gold",
            target_object=req.metric or "unknown",
            expected_value=str(s),
            actual_value=str(t),
            status=ValidationStatus.PASS if diff_pct <= 1 else ValidationStatus.FAIL,
            message=f"Diff: {diff_pct:.2f}%",
        )

    @staticmethod
    def _build_report(story_id: str, phase: str, checks: list[ValidationCheck]) -> ValidationReport:
        failures = [c.check_name for c in checks if c.status == ValidationStatus.FAIL]
        warnings = [c.check_name for c in checks if c.status == ValidationStatus.WARN]
        overall = (
            ValidationStatus.FAIL if failures
            else ValidationStatus.WARN if warnings
            else ValidationStatus.PASS
        )
        return ValidationReport(
            story_id=story_id,
            phase=phase,
            overall_status=overall,
            checks=checks,
            blocking_failures=failures,
            warnings=warnings,
        )
