"""Data Test Executor — Runs SQL validation tests against Synapse/SQL databases.

Executes each test query, compares results against expected values,
and returns a structured result set. Runs entirely server-side (no browser needed).
"""

from __future__ import annotations

import logging
import os
import re
import time

import pyodbc

logger = logging.getLogger(__name__)

# SQL keywords that must never appear in LLM-generated test queries
_FORBIDDEN_SQL_PATTERNS = re.compile(
    r"\b(DROP|DELETE|INSERT|UPDATE|ALTER|CREATE|TRUNCATE|EXEC|EXECUTE|MERGE|GRANT|REVOKE|DENY|xp_|sp_)\b",
    re.IGNORECASE,
)


def _sanitize_sql(sql: str) -> str:
    """Validate that LLM-generated SQL only contains SELECT statements.

    Raises ValueError if forbidden keywords are found.
    """
    # Strip comments (-- and /* ... */)
    stripped = re.sub(r"--[^\n]*", "", sql)
    stripped = re.sub(r"/\*.*?\*/", "", stripped, flags=re.DOTALL)

    match = _FORBIDDEN_SQL_PATTERNS.search(stripped)
    if match:
        raise ValueError(
            f"SQL query contains forbidden keyword '{match.group()}'. "
            "Only SELECT queries are allowed for data tests."
        )
    return sql


class DataTestExecutor:
    """Execute SQL-based data tests against Azure Synapse or Azure SQL."""

    def __init__(self):
        self._synapse_endpoint = os.environ.get("SYNAPSE_SQL_ENDPOINT", "")
        self._synapse_db = os.environ.get("SYNAPSE_SQL_DATABASE", "master")
        self._sql_user = os.environ.get("SQL_ADMIN_USER", "sqladmin")
        self._sql_password = os.environ.get("SQL_ADMIN_PASSWORD", "")
        self._source_server = os.environ.get("SOURCE_DB_SERVER", "")
        self._source_db = os.environ.get("SOURCE_DB_NAME", "")

    def _get_connection(self, target: str = "synapse") -> pyodbc.Connection:
        if target == "source":
            server = self._source_server
            db = self._source_db
        else:
            server = self._synapse_endpoint
            db = self._synapse_db

        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=tcp:{server},1433;"
            f"DATABASE={db};"
            f"UID={self._sql_user};"
            f"PWD={self._sql_password};"
            f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10;"
        )
        conn = pyodbc.connect(conn_str, timeout=10)
        conn.autocommit = True
        return conn

    def execute_tests(self, test_plan: dict, log_callback=None) -> dict:
        """Run all data tests with categorized logging."""
        tests = test_plan.get("tests", [])
        results = []
        passed = 0
        failed = 0
        errors = 0
        start_time = time.time()
        log_entries = []

        def log(level: str, msg: str):
            entry = {"ts": time.strftime("%H:%M:%S"), "level": level, "message": msg}
            log_entries.append(entry)
            logger.info("[%s] %s", level, msg)
            if log_callback:
                log_callback(level, msg)

        suite_name = test_plan.get("test_suite_name", "Data Tests")
        log("info", f"{'='*50}")
        log("info", f"DATA TEST SUITE: {suite_name}")
        log("info", f"Total tests: {len(tests)}")
        log("info", f"Target objects: {', '.join(test_plan.get('target_objects', []))}")
        log("info", f"{'='*50}")

        # Group tests by category for organized execution
        by_category = {}
        for t in tests:
            cat = t.get("category", "uncategorized")
            by_category.setdefault(cat, []).append(t)

        for category, cat_tests in by_category.items():
            cat_label = category.replace("_", " ").title()
            log("info", "")
            log("info", f"--- Category: {cat_label} ({len(cat_tests)} tests) ---")

            for test in cat_tests:
                test_id = test.get("id", "")
                test_name = test.get("name", "")
                priority = test.get("priority", "")

                log("run", f"  [{test_id}] {test_name} (priority: {priority})")
                log("info", f"         SQL: {test.get('sql', '')[:120]}...")

                result = self._run_single_test(test)
                results.append(result)

                if result["status"] == "passed":
                    passed += 1
                    log("ok", f"  PASSED -- actual: {result.get('actual_value')} ({result.get('duration')}s)")
                elif result["status"] == "failed":
                    failed += 1
                    log("err", f"  FAILED -- {result.get('message')} ({result.get('duration')}s)")
                else:
                    errors += 1
                    log("warn", f"  ERROR -- {result.get('message', '')[:100]} ({result.get('duration')}s)")

            cat_passed = sum(1 for r in results if r.get("category") == category and r["status"] == "passed")
            log("info", f"  {cat_label}: {cat_passed}/{len(cat_tests)} passed")

        elapsed = int(time.time() - start_time)
        total = passed + failed + errors

        log("info", "")
        log("info", f"{'='*50}")
        log("ok" if failed == 0 else "err", f"RESULTS: {passed}/{total} passed, {failed} failed, {errors} errors ({elapsed}s)")
        log("info", f"{'='*50}")

        category_summary = {}
        for r in results:
            cat = r.get("category", "uncategorized")
            if cat not in category_summary:
                category_summary[cat] = {"passed": 0, "failed": 0, "errors": 0}
            category_summary[cat][r["status"]] = category_summary[cat].get(r["status"], 0) + 1

        return {
            "status": "completed",
            "passed": passed,
            "failed": failed,
            "errors": errors,
            "total": total,
            "elapsed_seconds": elapsed,
            "test_results": results,
            "log_entries": log_entries,
            "category_summary": category_summary,
        }

    def _run_single_test(self, test: dict) -> dict:
        """Execute a single SQL test and evaluate the result."""
        test_id = test.get("id", "")
        name = test.get("name", "")
        sql = test.get("sql", "")
        expected_type = test.get("expected_type", "greater_than")
        expected_value = test.get("expected_value", 0)

        if not sql:
            return {
                "id": test_id,
                "name": name,
                "status": "error",
                "message": "No SQL query provided",
                "duration": 0,
                "actual_value": None,
            }

        # Sanitize LLM-generated SQL — only SELECT queries allowed
        try:
            sql = _sanitize_sql(sql)
        except ValueError as ve:
            return {
                "id": test_id,
                "name": name,
                "status": "error",
                "message": str(ve),
                "duration": 0,
                "actual_value": None,
                "category": test.get("category", ""),
            }

        start = time.time()
        try:
            # Determine target database from SQL content
            target = "synapse"
            sql_lower = sql.lower()
            if any(t in sql_lower for t in ["sales.", "dbo.", "source"]):
                if not any(t in sql_lower for t in ["bronze.", "silver.", "gold."]):
                    target = "source"

            conn = self._get_connection(target)
            try:
                # Wrap in a read-only transaction for safety
                conn.autocommit = False
                cursor = conn.cursor()
                cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                cursor.execute("BEGIN TRANSACTION")
                cursor.execute(sql)
                row = cursor.fetchone()
                conn.rollback()  # Always rollback — tests should never write
            finally:
                conn.close()

            duration = round(time.time() - start, 2)

            if row is None:
                actual = None
            elif len(row) == 1:
                actual = row[0]
            else:
                actual = list(row)

            # Evaluate result
            passed = self._evaluate(actual, expected_type, expected_value)

            return {
                "id": test_id,
                "name": name,
                "status": "passed" if passed else "failed",
                "duration": duration,
                "actual_value": actual,
                "expected_type": expected_type,
                "expected_value": expected_value,
                "message": "" if passed else f"Expected {expected_type} {expected_value}, got {actual}",
                "category": test.get("category", ""),
            }

        except Exception as e:
            duration = round(time.time() - start, 2)
            logger.warning("Data test '%s' error: %s", name, str(e)[:150])
            return {
                "id": test_id,
                "name": name,
                "status": "error",
                "duration": duration,
                "actual_value": None,
                "message": str(e)[:200],
                "category": test.get("category", ""),
            }

    def _evaluate(self, actual, expected_type: str, expected_value) -> bool:
        """Compare actual result against expected threshold."""
        if actual is None:
            return expected_type == "equals_zero"

        try:
            actual_num = float(actual) if not isinstance(actual, (int, float)) else actual
            expected_num = float(expected_value) if not isinstance(expected_value, (int, float)) else expected_value
        except (ValueError, TypeError):
            actual_num = actual
            expected_num = expected_value

        if expected_type == "equals":
            return actual_num == expected_num
        elif expected_type == "equals_zero":
            return actual_num == 0
        elif expected_type == "greater_than":
            return actual_num > expected_num
        elif expected_type == "less_than":
            return actual_num < expected_num
        elif expected_type == "not_empty":
            return actual is not None and actual != 0 and actual != ""
        elif expected_type == "boolean":
            return bool(actual) == bool(expected_value)
        else:
            return actual_num == expected_num
