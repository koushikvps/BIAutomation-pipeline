"""Universal Story Interpreter: normalizes any story format into StoryContract JSON.

Handles:
- Gherkin (Given/When/Then)
- Plain English business requirements
- Bullet-point acceptance criteria
- Technical specifications with table names
- Mixed format (some structured, some free-text)
- Stories from any project/team, not just sales domain
"""

from __future__ import annotations

import json
import logging
import re
from typing import Optional

from .config import AppConfig
from .llm_client import LLMClient

logger = logging.getLogger(__name__)

INTERPRETER_SYSTEM_PROMPT = """You are a Universal Data Engineering Story Interpreter. Your job is to read ANY business requirement — regardless of format, domain, or writing style — and produce a structured specification for building a medallion data architecture (Bronze/Silver/Gold).

You MUST handle ALL of these input formats:

1. GHERKIN FORMAT:
   "Given I am a regional sales manager
    When I view the quarterly dashboard
    Then I should see revenue by region and product category"

2. PLAIN ENGLISH:
   "We need to understand customer churn patterns across product categories for the last 2 years"

3. BULLET POINTS:
   "- Customer name, email, last order date
    - Total spend per customer
    - Segment by high/medium/low value"

4. TECHNICAL SPEC:
   "Source: sales.orders, sales.customers
    Join on customer_id
    Aggregate: SUM(total_amount) GROUP BY region"

5. MIXED / VAGUE:
   "The finance team wants a monthly P&L view. John said the data is in SAP somewhere."

INTERPRETATION RULES:

A) SOURCE TABLE INFERENCE:
   - If tables are explicitly named → use them directly
   - If tables are NOT named but domain is clear → infer likely table names
   - If domain is ambiguous → list candidate tables with a "confidence" field
   - ALWAYS prefix with likely schema (e.g., "sales.", "hr.", "finance.")
   - If you cannot determine ANY tables, set source_tables to [] and add a note in business_objective

B) METRIC INFERENCE:
   - "revenue" / "sales" / "spend" → SUM(amount/total/price * quantity)
   - "count" / "how many" / "number of" → COUNT(DISTINCT entity)
   - "average" / "avg" / "mean" → AVG(column)
   - "growth" / "YoY" / "trend" → Implies time-series aggregation
   - "rate" / "ratio" / "percentage" → Division of two measures
   - "churn" → Customers with no orders in period X

C) DIMENSION INFERENCE:
   - "by region" / "per region" → Region dimension
   - "over time" / "monthly" / "quarterly" → Date dimension with appropriate grain
   - "per customer" / "by customer" → Customer dimension
   - "by product" / "by category" → Product/Category dimension

D) FILTER INFERENCE:
   - "exclude cancelled" → WHERE status != 'Cancelled'
   - "last 12 months" / "last 2 years" → Date filter
   - "active customers only" → WHERE is_active = 1 or recent order filter
   - "top 10" → ORDER BY + LIMIT pattern

E) GHERKIN MAPPING:
   - "Given" → Context (who the user is, what system they use) → helps infer source_system
   - "When" → Trigger/action → helps infer the view purpose and grain
   - "Then" → Expected output → maps to metrics, dimensions, columns
   - "And" → Additional requirements → more metrics, filters, or acceptance criteria

F) CONFIDENCE SCORING:
   For each inferred field, assign confidence:
   - "high" = explicitly stated in the story
   - "medium" = strongly implied by context
   - "low" = best guess, may need human confirmation

OUTPUT FORMAT (strict JSON):
{
  "story_id": "STORY-<id>",
  "title": "concise title",
  "business_objective": "clear statement of what the business needs and why",
  "source_system": "inferred or explicit source system name",
  "source_tables": ["schema.table1", "schema.table2"],
  "dimensions": ["Dimension1", "Dimension2"],
  "metrics": ["SUM(column) AS MetricName", "COUNT(DISTINCT col) AS CountName"],
  "filters": ["condition1", "condition2"],
  "grain": "daily / weekly / monthly / per-customer / etc.",
  "joins": ["table1.col = table2.col"],
  "acceptance_criteria": ["criterion 1", "criterion 2"],
  "target_schema": "gold",
  "target_view_name": "vw_domain_descriptive_name",
  "priority": "high | medium | low",
  "interpretation_notes": "explanation of what was inferred vs explicit",
  "confidence": {
    "source_tables": "high | medium | low",
    "metrics": "high | medium | low",
    "dimensions": "high | medium | low",
    "overall": "high | medium | low"
  }
}

CRITICAL RULES:
- NEVER return empty source_tables without an explanation in interpretation_notes
- If unsure about tables, provide your best guess AND note the uncertainty
- Acceptance criteria should always include at minimum: "Row count > 0", "No null values in key columns"
- target_view_name must be descriptive and follow: vw_{domain}_{what_it_shows}
- Return ONLY valid JSON, no markdown fences, no explanation outside the JSON
"""

# Patterns for detecting story format
GHERKIN_PATTERN = re.compile(r'\b(given|when|then|and|but)\b\s+', re.IGNORECASE | re.MULTILINE)
TABLE_PATTERN = re.compile(r'(\w+)\.(\w+)', re.IGNORECASE)
METRIC_KEYWORDS = re.compile(r'\b(revenue|sales|count|total|average|sum|growth|churn|spend|profit|margin|rate|ratio|volume)\b', re.IGNORECASE)


class StoryInterpreter:
    """Normalizes any story format into a StoryContract-compatible dict."""

    def __init__(self, config: AppConfig = None):
        self._config = config
        self._llm = LLMClient(config) if config else None

    def detect_format(self, text: str) -> str:
        """Detect the story format."""
        gherkin_matches = len(GHERKIN_PATTERN.findall(text))
        table_matches = len(TABLE_PATTERN.findall(text))
        bullet_matches = text.count('\n-') + text.count('\n*') + text.count('\n•')

        if gherkin_matches >= 3:
            return "gherkin"
        elif table_matches >= 2 and any(kw in text.lower() for kw in ['source', 'join', 'aggregate', 'group by']):
            return "technical"
        elif bullet_matches >= 3:
            return "bullet_points"
        else:
            return "free_text"

    def interpret(self, text: str, work_item_id: int = 0, title: str = "",
                  acceptance_criteria: str = "", priority: str = "medium",
                  source_db_schema: Optional[list[dict]] = None) -> dict:
        """Interpret any story format into StoryContract JSON.

        Args:
            text: The story text in any format
            work_item_id: ADO work item ID (0 for non-ADO stories)
            title: Story title (if separate from text)
            acceptance_criteria: Separate acceptance criteria text
            priority: Priority level
            source_db_schema: Optional list of available tables/columns from the source DB
                              [{"schema": "sales", "table": "customers", "columns": ["id", "name"]}]
        """
        story_format = self.detect_format(text)
        logger.info("Story format detected: %s for WI-%s", story_format, work_item_id)

        # Build context about available source tables (if provided)
        schema_context = ""
        if source_db_schema:
            schema_context = "\n\nAVAILABLE SOURCE TABLES (use these for source_tables):\n"
            for t in source_db_schema:
                cols = ", ".join(t.get("columns", [])[:15])
                schema_context += f"  - {t['schema']}.{t['table']}: [{cols}]\n"

        # Build the user prompt with all context
        user_prompt = f"""STORY FORMAT: {story_format}
WORK ITEM ID: {work_item_id}
TITLE: {title}
PRIORITY: {priority}

STORY TEXT:
{text}

ACCEPTANCE CRITERIA:
{acceptance_criteria or '(none provided)'}
{schema_context}
Please interpret this story and return the structured JSON specification."""

        if not self._llm:
            logger.warning("No LLM client available, returning basic extraction")
            return self._basic_extract(text, work_item_id, title, priority)

        try:
            result = self._llm.chat_json(
                system_prompt=INTERPRETER_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                max_tokens=4096,
            )
            result["story_id"] = f"STORY-{work_item_id}" if work_item_id else result.get("story_id", "STORY-0")
            result["_format_detected"] = story_format
            result["_interpreted_by"] = "universal_story_interpreter"

            # Validate minimum fields
            if not result.get("source_tables"):
                result.setdefault("interpretation_notes", "")
                result["interpretation_notes"] += " WARNING: No source tables could be determined."
            if not result.get("target_view_name"):
                domain = result.get("source_system", "data").lower().replace(" ", "_")
                result["target_view_name"] = f"vw_{domain}_analysis"

            logger.info("Story interpreted: %d tables, %d metrics, confidence=%s",
                        len(result.get("source_tables", [])),
                        len(result.get("metrics", [])),
                        result.get("confidence", {}).get("overall", "unknown"))
            return result

        except Exception as e:
            logger.error("Story interpretation failed for WI-%s: %s", work_item_id, e)
            return self._basic_extract(text, work_item_id, title, priority)

    def interpret_gherkin(self, text: str) -> dict:
        """Parse Gherkin format into structured components before LLM call."""
        components = {"given": [], "when": [], "then": [], "and": [], "but": []}
        current_section = None
        for line in text.split("\n"):
            line = line.strip()
            lower = line.lower()
            for keyword in ["given", "when", "then", "and", "but"]:
                if lower.startswith(keyword + " "):
                    current_section = keyword
                    components[keyword].append(line[len(keyword)+1:].strip())
                    break
        return components

    def get_available_tables(self) -> list[dict]:
        """Query the source DB for available tables and columns to provide as context."""
        try:
            import pyodbc
            import os
            driver = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
            server = os.environ.get("SOURCE_DB_SERVER", "")
            database = os.environ.get("SOURCE_DB_NAME", "")
            user = os.environ.get("SYNAPSE_SQL_USER", "sqladmin")
            password = os.environ.get("SYNAPSE_SQL_PASSWORD", "")
            if not server or not database:
                return []
            conn_str = (
                f"DRIVER={{{driver}}};SERVER=tcp:{server},1433;DATABASE={database};"
                f"UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10;"
            )
            conn = pyodbc.connect(conn_str, timeout=10)
            cur = conn.cursor()
            cur.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'config', 'audit', 'catalog')
                ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
            """)
            tables = {}
            for schema, table, column in cur.fetchall():
                key = f"{schema}.{table}"
                if key not in tables:
                    tables[key] = {"schema": schema, "table": table, "columns": []}
                tables[key]["columns"].append(column)
            conn.close()
            return list(tables.values())
        except Exception as e:
            logger.warning("Could not fetch source schema: %s", e)
            return []

    def _basic_extract(self, text: str, wi_id: int, title: str, priority: str) -> dict:
        """Fallback: basic regex extraction when LLM is unavailable."""
        tables = []
        for schema, table in TABLE_PATTERN.findall(text):
            if schema.lower() not in ("e", "g", "i", "vs", "etc"):
                tables.append(f"{schema}.{table}")

        metrics = [m.group(0) for m in METRIC_KEYWORDS.finditer(text)]

        return {
            "story_id": f"STORY-{wi_id}" if wi_id else "STORY-0",
            "title": title or text[:80],
            "business_objective": text,
            "source_system": tables[0].split(".")[0] if tables else "unknown",
            "source_tables": list(set(tables)),
            "dimensions": [],
            "metrics": metrics,
            "filters": [],
            "grain": "",
            "joins": [],
            "acceptance_criteria": ["Row count > 0"],
            "target_schema": "gold",
            "target_view_name": None,
            "priority": priority,
            "_format_detected": "basic_fallback",
            "_interpreted_by": "basic_regex",
        }
