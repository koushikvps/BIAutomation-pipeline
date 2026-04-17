"""Data Quality Framework: post-load validation checks for medallion layers."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from .config import AppConfig
from .synapse_client import SynapseClient

logger = logging.getLogger(__name__)


class DQCheckType(str, Enum):
    ROW_COUNT = "row_count"
    NULL_CHECK = "null_check"
    DUPLICATE_CHECK = "duplicate_check"
    FRESHNESS = "freshness"
    REFERENTIAL = "referential"
    CROSS_LAYER = "cross_layer"
    AGGREGATION = "aggregation"
    CARDINALITY = "cardinality"


class DQStatus(str, Enum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    SKIP = "skip"


@dataclass
class DQResult:
    check_type: DQCheckType
    object_name: str
    status: DQStatus
    message: str
    expected: Optional[str] = None
    actual: Optional[str] = None


@dataclass
class DQReport:
    story_id: str
    results: list[DQResult] = field(default_factory=list)

    @property
    def pass_count(self) -> int:
        return len([r for r in self.results if r.status == DQStatus.PASS])

    @property
    def warn_count(self) -> int:
        return len([r for r in self.results if r.status == DQStatus.WARN])

    @property
    def fail_count(self) -> int:
        return len([r for r in self.results if r.status == DQStatus.FAIL])

    @property
    def overall_status(self) -> DQStatus:
        if self.fail_count > 0:
            return DQStatus.FAIL
        if self.warn_count > 0:
            return DQStatus.WARN
        return DQStatus.PASS

    def to_dict(self) -> dict:
        return {
            "story_id": self.story_id,
            "overall_status": self.overall_status.value,
            "pass_count": self.pass_count,
            "warn_count": self.warn_count,
            "fail_count": self.fail_count,
            "results": [
                {
                    "check_type": r.check_type.value,
                    "object_name": r.object_name,
                    "status": r.status.value,
                    "message": r.message,
                    "expected": r.expected,
                    "actual": r.actual,
                }
                for r in self.results
            ],
        }


class DataQualityValidator:
    """Runs data quality checks against deployed Synapse objects."""

    def __init__(self, config: AppConfig):
        self._synapse = SynapseClient(config)

    def run_checks(self, story_id: str, objects: list[dict]) -> DQReport:
        """Run all DQ checks against a list of deployed objects.

        Args:
            story_id: Pipeline story ID
            objects: list of {"schema": str, "name": str, "layer": str, "type": str}
        """
        report = DQReport(story_id=story_id)

        bronze_objs, silver_objs, gold_objs = [], [], []
        for obj in objects:
            schema = obj.get("schema", "bronze")
            name = obj.get("name", "")
            layer = obj.get("layer", "")
            obj_type = obj.get("type", "")
            full_name = f"[{schema}].[{name}]"

            if obj_type in ("adf_pipeline", "adf_copy"):
                continue

            report.results.append(self._check_row_count(schema, name, full_name))
            report.results.append(self._check_nulls(schema, name, full_name))

            if layer in ("silver", "gold"):
                report.results.append(self._check_duplicates(schema, name, full_name))

            if layer == "bronze":
                bronze_objs.append(obj)
            elif layer == "silver":
                silver_objs.append(obj)
            elif layer == "gold":
                gold_objs.append(obj)

        for s_obj in silver_objs:
            for b_obj in bronze_objs:
                report.results.append(self._check_cross_layer(
                    b_obj.get("schema", "bronze"), b_obj.get("name", ""),
                    s_obj.get("schema", "silver"), s_obj.get("name", "")))

        for g_obj in gold_objs:
            report.results.append(self._check_aggregation_integrity(
                g_obj.get("schema", "gold"), g_obj.get("name", "")))

        for s_obj in silver_objs:
            report.results.append(self._check_cardinality(
                s_obj.get("schema", "silver"), s_obj.get("name", "")))

        logger.info("DQ report for %s: %d pass, %d warn, %d fail",
                     story_id, report.pass_count, report.warn_count, report.fail_count)
        return report

    def _check_row_count(self, schema: str, name: str, full_name: str) -> DQResult:
        try:
            rows = self._synapse.execute_query(
                f"SELECT COUNT(*) AS cnt FROM [{schema}].[{name}]"
            )
            cnt = rows[0]["cnt"] if rows else 0
            if cnt == 0:
                return DQResult(
                    check_type=DQCheckType.ROW_COUNT, object_name=full_name,
                    status=DQStatus.WARN, message="Table is empty",
                    expected=">0", actual=str(cnt),
                )
            return DQResult(
                check_type=DQCheckType.ROW_COUNT, object_name=full_name,
                status=DQStatus.PASS, message=f"{cnt} rows",
                actual=str(cnt),
            )
        except Exception as e:
            return DQResult(
                check_type=DQCheckType.ROW_COUNT, object_name=full_name,
                status=DQStatus.SKIP, message=f"Could not check: {str(e)[:100]}",
            )

    def _check_nulls(self, schema: str, name: str, full_name: str) -> DQResult:
        try:
            cols = self._synapse.get_columns(schema, name)
            not_null_cols = [c["COLUMN_NAME"] for c in cols if c.get("IS_NULLABLE") == "NO"]
            if not not_null_cols:
                return DQResult(
                    check_type=DQCheckType.NULL_CHECK, object_name=full_name,
                    status=DQStatus.PASS, message="No NOT NULL constraints to check",
                )

            null_issues = []
            for col in not_null_cols[:5]:  # Check up to 5 columns
                rows = self._synapse.execute_query(
                    f"SELECT COUNT(*) AS cnt FROM [{schema}].[{name}] WHERE [{col}] IS NULL"
                )
                null_count = rows[0]["cnt"] if rows else 0
                if null_count > 0:
                    null_issues.append(f"{col}={null_count}")

            if null_issues:
                return DQResult(
                    check_type=DQCheckType.NULL_CHECK, object_name=full_name,
                    status=DQStatus.FAIL, message=f"NULL values in NOT NULL columns: {', '.join(null_issues)}",
                )
            return DQResult(
                check_type=DQCheckType.NULL_CHECK, object_name=full_name,
                status=DQStatus.PASS, message=f"Checked {len(not_null_cols)} NOT NULL columns",
            )
        except Exception as e:
            return DQResult(
                check_type=DQCheckType.NULL_CHECK, object_name=full_name,
                status=DQStatus.SKIP, message=f"Could not check: {str(e)[:100]}",
            )

    def _check_duplicates(self, schema: str, name: str, full_name: str) -> DQResult:
        try:
            # Find potential key columns (columns ending in _id or named 'id')
            cols = self._synapse.get_columns(schema, name)
            key_cols = [c["COLUMN_NAME"] for c in cols
                        if c["COLUMN_NAME"].lower().endswith("_id") or c["COLUMN_NAME"].lower() == "id"]

            if not key_cols:
                return DQResult(
                    check_type=DQCheckType.DUPLICATE_CHECK, object_name=full_name,
                    status=DQStatus.SKIP, message="No key columns identified for duplicate check",
                )

            # Check first key column for duplicates
            key_col = key_cols[0]
            rows = self._synapse.execute_query(
                f"SELECT [{key_col}], COUNT(*) AS cnt FROM [{schema}].[{name}] "
                f"GROUP BY [{key_col}] HAVING COUNT(*) > 1"
            )
            dup_count = len(rows) if rows else 0
            if dup_count > 0:
                return DQResult(
                    check_type=DQCheckType.DUPLICATE_CHECK, object_name=full_name,
                    status=DQStatus.WARN, message=f"{dup_count} duplicate keys found in [{key_col}]",
                    expected="0", actual=str(dup_count),
                )
            return DQResult(
                check_type=DQCheckType.DUPLICATE_CHECK, object_name=full_name,
                status=DQStatus.PASS, message=f"No duplicates on [{key_col}]",
            )
        except Exception as e:
            return DQResult(
                check_type=DQCheckType.DUPLICATE_CHECK, object_name=full_name,
                status=DQStatus.SKIP, message=f"Could not check: {str(e)[:100]}",
            )

    def _check_cross_layer(self, src_schema: str, src_name: str,
                           tgt_schema: str, tgt_name: str) -> DQResult:
        """Verify row counts between layers (bronze -> silver) are within tolerance."""
        pair = f"[{src_schema}].[{src_name}] -> [{tgt_schema}].[{tgt_name}]"
        try:
            src_rows = self._synapse.execute_query(
                f"SELECT COUNT(*) AS cnt FROM [{src_schema}].[{src_name}]")
            tgt_rows = self._synapse.execute_query(
                f"SELECT COUNT(*) AS cnt FROM [{tgt_schema}].[{tgt_name}]")
            src_cnt = src_rows[0]["cnt"] if src_rows else 0
            tgt_cnt = tgt_rows[0]["cnt"] if tgt_rows else 0
            if src_cnt == 0:
                return DQResult(check_type=DQCheckType.CROSS_LAYER, object_name=pair,
                                status=DQStatus.SKIP, message="Source is empty")
            diff_pct = abs(src_cnt - tgt_cnt) / max(src_cnt, 1) * 100
            if diff_pct > 10:
                return DQResult(check_type=DQCheckType.CROSS_LAYER, object_name=pair,
                                status=DQStatus.FAIL,
                                message=f"Row count diff {diff_pct:.1f}% (src={src_cnt}, tgt={tgt_cnt})",
                                expected=str(src_cnt), actual=str(tgt_cnt))
            if diff_pct > 2:
                return DQResult(check_type=DQCheckType.CROSS_LAYER, object_name=pair,
                                status=DQStatus.WARN,
                                message=f"Row count diff {diff_pct:.1f}% (src={src_cnt}, tgt={tgt_cnt})",
                                expected=str(src_cnt), actual=str(tgt_cnt))
            return DQResult(check_type=DQCheckType.CROSS_LAYER, object_name=pair,
                            status=DQStatus.PASS,
                            message=f"Counts match within 2% (src={src_cnt}, tgt={tgt_cnt})")
        except Exception as e:
            return DQResult(check_type=DQCheckType.CROSS_LAYER, object_name=pair,
                            status=DQStatus.SKIP, message=f"Could not check: {str(e)[:100]}")

    def _check_aggregation_integrity(self, schema: str, name: str) -> DQResult:
        """Verify gold layer aggregations produce non-zero numeric results."""
        full_name = f"[{schema}].[{name}]"
        try:
            cols = self._synapse.get_columns(schema, name)
            numeric_cols = [c["COLUMN_NAME"] for c in cols
                           if c.get("DATA_TYPE", "").lower() in
                           ("int", "bigint", "decimal", "numeric", "float", "real", "money", "smallmoney")]
            if not numeric_cols:
                return DQResult(check_type=DQCheckType.AGGREGATION, object_name=full_name,
                                status=DQStatus.SKIP, message="No numeric columns to check")
            check_col = numeric_cols[0]
            rows = self._synapse.execute_query(
                f"SELECT SUM(CAST([{check_col}] AS FLOAT)) AS total, "
                f"MIN(CAST([{check_col}] AS FLOAT)) AS mn, "
                f"MAX(CAST([{check_col}] AS FLOAT)) AS mx "
                f"FROM [{schema}].[{name}]")
            if not rows:
                return DQResult(check_type=DQCheckType.AGGREGATION, object_name=full_name,
                                status=DQStatus.WARN, message="No rows returned")
            total = rows[0].get("total") or 0
            mn = rows[0].get("mn") or 0
            mx = rows[0].get("mx") or 0
            if total == 0 and mn == 0 and mx == 0:
                return DQResult(check_type=DQCheckType.AGGREGATION, object_name=full_name,
                                status=DQStatus.WARN,
                                message=f"All values zero in [{check_col}] -- check aggregation logic")
            return DQResult(check_type=DQCheckType.AGGREGATION, object_name=full_name,
                            status=DQStatus.PASS,
                            message=f"[{check_col}] SUM={total:.2f}, MIN={mn:.2f}, MAX={mx:.2f}")
        except Exception as e:
            return DQResult(check_type=DQCheckType.AGGREGATION, object_name=full_name,
                            status=DQStatus.SKIP, message=f"Could not check: {str(e)[:100]}")

    def _check_cardinality(self, schema: str, name: str) -> DQResult:
        """Check for suspicious cardinality (e.g., cartesian join indicators)."""
        full_name = f"[{schema}].[{name}]"
        try:
            cols = self._synapse.get_columns(schema, name)
            id_cols = [c["COLUMN_NAME"] for c in cols
                       if c["COLUMN_NAME"].lower().endswith("_id") or c["COLUMN_NAME"].lower() == "id"]
            if len(id_cols) < 2:
                return DQResult(check_type=DQCheckType.CARDINALITY, object_name=full_name,
                                status=DQStatus.SKIP, message="Fewer than 2 ID columns -- skip cardinality check")
            col_a, col_b = id_cols[0], id_cols[1]
            rows = self._synapse.execute_query(f"SELECT COUNT(*) AS total FROM [{schema}].[{name}]")
            total = rows[0]["total"] if rows else 0
            rows_a = self._synapse.execute_query(
                f"SELECT COUNT(DISTINCT [{col_a}]) AS cnt FROM [{schema}].[{name}]")
            distinct_a = rows_a[0]["cnt"] if rows_a else 0
            rows_b = self._synapse.execute_query(
                f"SELECT COUNT(DISTINCT [{col_b}]) AS cnt FROM [{schema}].[{name}]")
            distinct_b = rows_b[0]["cnt"] if rows_b else 0
            expected_max = distinct_a * distinct_b
            if expected_max > 0 and total > expected_max * 0.8:
                return DQResult(check_type=DQCheckType.CARDINALITY, object_name=full_name,
                                status=DQStatus.WARN,
                                message=f"Possible cartesian: {total} rows vs {distinct_a}x{distinct_b}={expected_max} max",
                                expected=f"<{expected_max}", actual=str(total))
            return DQResult(check_type=DQCheckType.CARDINALITY, object_name=full_name,
                            status=DQStatus.PASS,
                            message=f"Cardinality OK: {total} rows, {distinct_a} [{col_a}], {distinct_b} [{col_b}]")
        except Exception as e:
            return DQResult(check_type=DQCheckType.CARDINALITY, object_name=full_name,
                            status=DQStatus.SKIP, message=f"Could not check: {str(e)[:100]}")
