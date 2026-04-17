"""Data Test Planner — AI generates data validation test scenarios.

Produces SQL-based test queries that validate data quality, transformation
accuracy, referential integrity, and schema compliance across any data layer.
"""

from __future__ import annotations

import json
import logging
import os

from shared.config import AppConfig
from shared.llm_client import LLMClient

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a senior data quality engineer. Given a user story about data,
generate comprehensive SQL-based test scenarios.

You will be provided with CUSTOM TEST CATEGORIES defined by the organization.
You MUST map each test to one of these categories. If no custom categories are provided,
use the default categories below.

For each test, provide:
1. A clear test name
2. Category — MUST match one of the provided custom category IDs
3. Priority: "critical", "high", "medium", "low"
4. The SQL query to run (must be valid for Azure Synapse / SQL Server)
5. Expected result type: "equals", "greater_than", "less_than", "equals_zero", "not_empty", "boolean"
6. Expected value (the threshold or exact value)
7. Description of what this test validates

Default categories (use ONLY if no custom categories provided):
- "completeness": All expected records exist
- "accuracy": Values match source/transformations correct
- "uniqueness": No duplicate records on key columns
- "consistency": Cross-layer alignment
- "validity": Values conform to business rules
- "referential_integrity": Foreign keys resolve
- "timeliness": Data freshness within SLA

IMPORTANT:
- Use fully qualified names: schema.table_name
- For cross-layer tests, compare Bronze vs Silver vs Gold
- For aggregation checks, verify SUM/COUNT matches between layers
- For referential integrity, use LEFT JOIN and check for NULLs
- For null checks, count nulls in required columns
- For duplicate checks, use GROUP BY HAVING COUNT(*) > 1
- Always include a completeness test for each table mentioned
- Read the category descriptions carefully and generate tests that match their intent

Return JSON:
{
  "test_suite_name": "Data Tests — <story>",
  "target_objects": ["schema.table1", "schema.table2"],
  "categories_used": ["completeness", "accuracy"],
  "tests": [
    {
      "id": "DT-001",
      "name": "Row count check for silver.sales_daily_summary",
      "category": "completeness",
      "priority": "critical",
      "sql": "SELECT COUNT(*) AS cnt FROM silver.sales_daily_summary",
      "expected_type": "greater_than",
      "expected_value": 0,
      "description": "Verify the silver table has data"
    },
    {
      "id": "DT-002",
      "name": "No nulls in required columns",
      "category": "accuracy",
      "priority": "high",
      "sql": "SELECT COUNT(*) AS null_count FROM silver.sales_daily_summary WHERE Region IS NULL OR TotalRevenue IS NULL",
      "expected_type": "equals_zero",
      "expected_value": 0,
      "description": "Required columns should have no null values"
    }
  ]
}
"""


class DataTestPlanner:
    def __init__(self, config: AppConfig):
        self._llm = LLMClient(config)
        self._config = config

    def plan_data_tests(
        self,
        story: dict,
        data_aspects: list[str] | None = None,
        custom_categories: list[dict] | None = None,
    ) -> dict:
        """Generate data test plan from story."""
        title = story.get("title", "")
        description = story.get("description", "")
        acceptance_criteria = story.get("acceptance_criteria", "")

        # Query existing objects for context
        context = self._get_db_context()

        # Build categories section
        if custom_categories:
            enabled = [c for c in custom_categories if c.get("enabled", True)]
            cats_text = "\n".join(
                f'  - "{c["id"]}": {c["name"]} — {c.get("description", "")}'
                for c in enabled
            )
            cats_instruction = f"""CUSTOM TEST CATEGORIES (MUST use these category IDs):
{cats_text}

Generate at least one test for each applicable category. Map tests to the category
that best matches based on the category description."""
        else:
            cats_instruction = "No custom categories provided. Use default categories from system prompt."

        user_prompt = f"""Generate data validation tests for this story:

Title: {title}
Description: {description}
Acceptance Criteria: {acceptance_criteria}

Data aspects to test: {json.dumps(data_aspects or [])}

{cats_instruction}

Available database objects for context:
{context}

Generate comprehensive SQL tests covering:
- Completeness checks for all tables mentioned
- Null checks on required/business-critical columns
- Duplicate checks on primary/unique columns
- Referential integrity between related tables
- Transformation accuracy (source vs target values)
- Cross-layer consistency (Bronze vs Silver vs Gold if applicable)
- Business rule validation from acceptance criteria
"""

        logger.info("Data Test Planner: generating tests for '%s'", title[:60])
        result = self._llm.chat_json(
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user_prompt,
            temperature=0.1,
            max_tokens=4096,
        )

        test_count = len(result.get("tests", []))
        categories = {}
        for t in result.get("tests", []):
            cat = t.get("category", "other")
            categories[cat] = categories.get(cat, 0) + 1

        logger.info(
            "Data Test Planner: generated %d tests — %s",
            test_count,
            ", ".join(f"{k}:{v}" for k, v in categories.items()),
        )
        return result

    def _get_db_context(self) -> str:
        """Get list of existing tables/views from Synapse for context."""
        try:
            import pyodbc
            conn_str = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER=tcp:{self._config.synapse_endpoint},1433;"
                f"DATABASE={self._config.synapse_database};"
                f"UID={os.environ.get('SQL_ADMIN_USER', 'sqladmin')};"
                f"PWD={os.environ.get('SQL_ADMIN_PASSWORD', '')};"
                f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=5;"
            )
            with pyodbc.connect(conn_str, timeout=5) as conn:
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT s.name AS schema_name, o.name AS object_name, o.type_desc
                    FROM sys.objects o JOIN sys.schemas s ON o.schema_id = s.schema_id
                    WHERE o.type IN ('U','V') AND s.name IN ('bronze','silver','gold','dbo','sales')
                    ORDER BY s.name, o.name
                """)
                rows = cursor.fetchall()
                if rows:
                    return "\n".join(f"  {r[0]}.{r[1]} ({r[2]})" for r in rows)
        except Exception as e:
            logger.warning("Could not query DB context: %s", e)

        return "  (Could not query database — generate tests based on story context)"
