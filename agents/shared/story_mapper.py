"""Maps Azure DevOps work items to StoryContract JSON for the pipeline.

Uses the Universal Story Interpreter for Gherkin, free-text, and any format.
Falls back to rule-based extraction for well-structured technical stories.
"""

from __future__ import annotations

import html
import logging
import os
import re

from .config import AppConfig
from .llm_client import LLMClient

logger = logging.getLogger(__name__)

MAPPER_PROMPT = """You are a data engineering assistant that converts Azure DevOps User Stories into structured BI modeling specifications.

Given an ADO work item with Title, Description, and Acceptance Criteria (in HTML), extract the following JSON:

{
  "story_id": "STORY-<work_item_id>",
  "title": "the title",
  "business_objective": "the business requirement from description",
  "source_system": "the source database/system name",
  "source_tables": ["schema.Table1", "schema.Table2"],
  "dimensions": ["Dim1", "Dim2"],
  "metrics": ["SUM(col) AS MetricName"],
  "filters": ["condition1", "condition2"],
  "grain": "the grain/granularity",
  "joins": ["Table1.Col = Table2.Col"],
  "acceptance_criteria": ["criterion 1", "criterion 2"],
  "target_schema": "gold",
  "target_view_name": "vw_descriptive_name",
  "priority": "high | medium | low"
}

Rules:
- Extract source tables from the description. Look for table names, schema references, database mentions.
- If source tables aren't explicitly listed, infer them from the metrics and dimensions mentioned.
- Map ADO priority (1=high, 2=medium, 3=low, 4=low).
- Strip all HTML tags from description and acceptance criteria.
- If joins aren't specified, infer them from common patterns (foreign keys, shared column names).
- The target_view_name should follow the pattern: vw_{domain}_{descriptive_name}
- Return ONLY valid JSON.
"""

# Patterns that indicate the story is NOT in a clean technical format
GHERKIN_PATTERN = re.compile(r'\b(given|when|then)\b\s+', re.IGNORECASE | re.MULTILINE)
BUSINESS_LANGUAGE = re.compile(r'\b(I want|I need|I should see|we need|as a|so that|in order to|dashboard|report|view|understand|analyze|track|monitor)\b', re.IGNORECASE)


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<li>', '\n- ', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html.unescape(text)
    return text.strip()


class StoryMapper:
    """Converts ADO work item fields into a StoryContract-compatible dict."""

    def __init__(self, config: AppConfig = None):
        self._config = config
        self._llm = LLMClient(config) if config else None

    def _needs_universal_interpreter(self, text: str) -> bool:
        """Detect if the story is in Gherkin, business language, or other non-technical format."""
        gherkin_hits = len(GHERKIN_PATTERN.findall(text))
        business_hits = len(BUSINESS_LANGUAGE.findall(text))
        table_hits = len(re.findall(r'(\w+)\.(\w+)', text))
        # Use interpreter if: Gherkin detected, or heavy business language with few explicit tables
        if gherkin_hits >= 2:
            return True
        if business_hits >= 3 and table_hits < 2:
            return True
        return False

    def map_work_item(self, wi_fields: dict) -> dict:
        """Convert ADO work item fields to story JSON.

        Routing logic:
        1. If story is Gherkin or business language → Universal Story Interpreter
        2. If story has explicit tables → rule-based extraction
        3. Fallback → LLM extraction with original prompt
        """
        title = wi_fields.get("title", "")
        description = strip_html(wi_fields.get("description", ""))
        acceptance = strip_html(wi_fields.get("acceptance_criteria", ""))
        work_item_id = wi_fields.get("id", 0)
        priority_num = wi_fields.get("priority", "2")

        priority_map = {"1": "high", "2": "medium", "3": "low", "4": "low"}
        priority = priority_map.get(str(priority_num), "medium")

        full_text = f"{title}\n{description}\n{acceptance}"

        # Route 1: Universal Interpreter for Gherkin / business language / vague stories
        if self._needs_universal_interpreter(full_text):
            logger.info("Routing WI-%s to Universal Story Interpreter (format: non-technical)", work_item_id)
            try:
                from .story_interpreter import StoryInterpreter
                interpreter = StoryInterpreter(self._config)
                # Provide source DB schema as context so the AI knows what tables exist
                available_tables = interpreter.get_available_tables()
                result = interpreter.interpret(
                    text=f"{description}\n\n{acceptance}",
                    work_item_id=work_item_id,
                    title=title,
                    acceptance_criteria=acceptance,
                    priority=priority,
                    source_db_schema=available_tables,
                )
                if result.get("source_tables"):
                    return result
                logger.warning("Universal interpreter returned no tables, falling through to rule-based")
            except Exception as e:
                logger.warning("Universal interpreter failed for WI-%s: %s, falling through", work_item_id, e)

        # Route 2: Rule-based extraction for well-structured technical stories
        story = self._rule_based_extract(work_item_id, title, description, acceptance, priority)
        if story and story.get("source_tables"):
            logger.info("Rule-based extraction succeeded for WI-%s", work_item_id)
            return story

        # Route 3: LLM extraction with original mapper prompt
        if self._llm:
            logger.info("Falling back to LLM extraction for WI-%s", work_item_id)
            return self._llm_extract(work_item_id, title, description, acceptance, priority)

        return story or self._empty_story(work_item_id, title, priority)

    def _rule_based_extract(self, wi_id: int, title: str, desc: str, acceptance: str, priority: str) -> dict | None:
        """Extract story fields using regex patterns."""
        story = {
            "story_id": f"STORY-{wi_id}",
            "title": title,
            "business_objective": desc,
            "source_system": "",
            "source_tables": [],
            "dimensions": [],
            "metrics": [],
            "filters": [],
            "grain": "",
            "joins": [],
            "acceptance_criteria": [line.strip().lstrip("- ") for line in acceptance.split("\n") if line.strip()],
            "target_schema": "gold",
            "target_view_name": None,
            "priority": priority,
        }

        # Extract source tables (pattern: schema.Table or [schema].[Table])
        # Only match lines that look like "Source tables: schema.Table, schema.Table2"
        # Avoid matching join conditions like "OrderHeader.OrderId = OrderDetail.OrderId"
        table_patterns = re.findall(r'(\w+)\.(\w+)', desc)
        skip_words = {"e", "g", "i", "vs", "etc", "org", "com", "net"}
        # Column names commonly found in join conditions
        column_names = {"orderid", "customerid", "productid", "id", "key", "code", "date", "name", "type"}
        seen = set()
        for schema, table in table_patterns:
            key = f"{schema}.{table}".lower()
            if key in seen:
                continue
            if schema.lower() in skip_words:
                continue
            # Skip if table part looks like a column name (join condition)
            if table.lower() in column_names or table.lower().endswith("id"):
                continue
            story["source_tables"].append(f"{schema}.{table}")
            seen.add(key)
            if not story["source_system"]:
                story["source_system"] = schema

        # Extract source system from keywords
        for pattern in [r'(?:from|source|database|system)[:\s]+(\w+)', r'(\w+DB)\b', r'(\w+)(?:\s+database)']:
            match = re.search(pattern, desc, re.IGNORECASE)
            if match and not story["source_system"]:
                story["source_system"] = match.group(1)

        # Extract metrics (SUM, COUNT, AVG patterns)
        metrics = re.findall(r'((?:SUM|COUNT|AVG|MIN|MAX)\s*\([^)]+\)\s*(?:AS\s+\w+)?)', desc, re.IGNORECASE)
        story["metrics"] = metrics

        # Extract dimensions
        dim_match = re.search(r'(?:by|group\s+by|broken\s+down\s+by|per)\s+([^.]+?)(?:\.|$)', desc, re.IGNORECASE)
        if dim_match:
            dims = re.split(r',\s*|\s+and\s+', dim_match.group(1))
            story["dimensions"] = [d.strip() for d in dims if d.strip()]

        return story if story["source_tables"] else None

    def _llm_extract(self, wi_id: int, title: str, desc: str, acceptance: str, priority: str) -> dict:
        """Use LLM to parse unstructured story text."""
        user_prompt = f"""Work Item ID: {wi_id}
Title: {title}
Priority: {priority}

Description:
{desc}

Acceptance Criteria:
{acceptance}
"""
        try:
            result = self._llm.chat_json(
                system_prompt=MAPPER_PROMPT,
                user_prompt=user_prompt,
                max_tokens=4096,
            )
            result["story_id"] = f"STORY-{wi_id}"
            return result
        except Exception as e:
            logger.error("LLM extraction failed for WI-%s: %s", wi_id, e)
            return self._empty_story(wi_id, title, priority)

    @staticmethod
    def _empty_story(wi_id: int, title: str, priority: str) -> dict:
        return {
            "story_id": f"STORY-{wi_id}",
            "title": title,
            "business_objective": title,
            "source_system": "unknown",
            "source_tables": [],
            "dimensions": [],
            "metrics": [],
            "filters": [],
            "grain": "",
            "joins": [],
            "acceptance_criteria": [],
            "target_schema": "gold",
            "target_view_name": None,
            "priority": priority,
        }
