"""Test Router Agent — AI classifies story and routes to UI, Data, or Both test paths.

Analyzes user story content to determine which test type(s) to run:
- UI Testing: Power Apps screens, buttons, forms, user flows
- Data Testing: Tables, SQL, transformations, data quality, medallion layers
- Both: Story has UI and data components
"""

from __future__ import annotations

import json
import logging

from shared.config import AppConfig
from shared.llm_client import LLMClient

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a QA routing agent. Given a user story, classify what type of testing is needed.

Analyze the story title, description, and acceptance criteria to determine:

1. "ui" — Story is about user interface, Power Apps screens, forms, buttons, navigation, user experience.
   Keywords: screen, form, button, click, navigate, display, page, canvas app, Power Apps, input, dropdown, gallery, submit, upload, UI, user interface

2. "data" — Story is about data pipelines, tables, transformations, SQL, data quality, medallion layers, ETL, reporting data.
   Keywords: table, column, SQL, Bronze, Silver, Gold, medallion, ETL, pipeline, transform, aggregate, join, view, stored procedure, data quality, row count, null, duplicate, Synapse, Dataverse, data flow

3. "both" — Story has BOTH UI and data components (e.g., user submits form AND data is validated in backend).

Return JSON:
{
  "test_type": "ui" | "data" | "both",
  "confidence": 0.0-1.0,
  "reasoning": "Brief explanation of why this classification",
  "ui_aspects": ["list of UI elements to test"] or [],
  "data_aspects": ["list of data checks to perform"] or []
}
"""


class TestRouter:
    def __init__(self, config: AppConfig):
        self._llm = LLMClient(config)

    def classify(self, story: dict) -> dict:
        """Classify story as UI, Data, or Both."""
        title = story.get("title", "")
        description = story.get("description", "")
        acceptance_criteria = story.get("acceptance_criteria", "")

        user_prompt = f"""Classify this story:

Title: {title}
Description: {description}
Acceptance Criteria: {acceptance_criteria}

What type of testing is needed: "ui", "data", or "both"?
"""

        logger.info("Test Router: classifying story '%s'", title[:60])
        result = self._llm.chat_json(
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user_prompt,
            temperature=0.0,
            max_tokens=512,
        )

        test_type = result.get("test_type", "both")
        logger.info(
            "Test Router: classified as '%s' (confidence: %s) — %s",
            test_type,
            result.get("confidence", "?"),
            result.get("reasoning", "")[:80],
        )
        return result
