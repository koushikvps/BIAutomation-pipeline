"""Test Planner Agent — AI reads user story and generates test scenarios.

Takes an ADO user story (title, description, acceptance criteria) and produces
structured test scenarios with steps and expected results, suitable for both
ADO Test Case creation and Playwright code generation.
"""

from __future__ import annotations

import json
import logging

from shared.config import AppConfig
from shared.llm_client import LLMClient

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a senior QA engineer specializing in Power Apps Canvas applications.
Given a user story with acceptance criteria, generate comprehensive test scenarios.

For each test scenario, provide:
1. A clear title
2. Category: "happy_path", "negative", "edge_case", "accessibility", "performance"
3. Priority: "critical", "high", "medium", "low"
4. Preconditions (what must be true before test starts)
5. Ordered test steps with:
   - action: what the tester does (click, type, navigate, verify, upload, etc.)
   - target: which Power Apps control (by label/name as user sees it)
   - value: data to input (if applicable)
   - expected: what should happen after this step
6. Test data requirements (what data is needed from SharePoint Excel)

IMPORTANT for Power Apps Canvas:
- Controls are identified by their visible label or aria-label
- Forms use gallery, text input, dropdown, date picker, toggle, button controls
- Navigation uses Screen references
- Data comes from Dataverse/SharePoint connectors
- Look for delegation warnings, loading states, error banners

Return JSON with this exact structure:
{
  "test_plan_name": "Story #<id> — <title>",
  "app_screens": ["Screen1", "Screen2"],
  "test_scenarios": [
    {
      "id": "TC-001",
      "title": "...",
      "category": "happy_path",
      "priority": "critical",
      "preconditions": ["User is logged in", "..."],
      "steps": [
        {"step": 1, "action": "navigate", "target": "ExpenseFormScreen", "value": null, "expected": "Expense form is displayed"},
        {"step": 2, "action": "type", "target": "Amount input", "value": "150.00", "expected": "Amount field shows 150.00"},
        {"step": 3, "action": "click", "target": "Submit button", "value": null, "expected": "Success message displayed"}
      ],
      "test_data": {"amount": "150.00", "category": "Travel"},
      "tags": ["smoke", "regression"]
    }
  ],
  "test_data_requirements": {
    "excel_file": "TestData.xlsx",
    "sheets": ["ExpenseData", "UserProfiles"],
    "columns_needed": {"ExpenseData": ["Amount", "Category", "Description"]}
  }
}
"""


class TestPlannerAgent:
    def __init__(self, config: AppConfig):
        self._llm = LLMClient(config)

    def plan_tests(self, story: dict, app_url: str = "") -> dict:
        """Generate test plan from ADO story."""
        story_id = story.get("story_id", "")
        work_item_id = story.get("work_item_id", 0)
        title = story.get("title", "")
        description = story.get("description", "")
        acceptance_criteria = story.get("acceptance_criteria", "")

        user_prompt = f"""User Story: {title}
Story ID: {story_id}
Work Item: #{work_item_id}

Description:
{description}

Acceptance Criteria:
{acceptance_criteria}

Power App URL: {app_url or 'Will be provided at execution time'}

Generate comprehensive test scenarios covering:
- All acceptance criteria as happy path tests
- Negative tests (invalid inputs, missing required fields)
- Edge cases (boundary values, special characters, max lengths)
- At least 1 accessibility check (screen reader, keyboard navigation)

Target: Power Apps Canvas application running in Microsoft Edge.
"""

        logger.info("Test Planner: generating scenarios for %s", story_id)
        result = self._llm.chat_json(
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user_prompt,
            temperature=0.1,
            max_tokens=4096,
        )

        # Enrich with story metadata
        result["story_id"] = story_id
        result["work_item_id"] = work_item_id
        result["app_url"] = app_url

        scenario_count = len(result.get("test_scenarios", []))
        categories = {}
        for tc in result.get("test_scenarios", []):
            cat = tc.get("category", "other")
            categories[cat] = categories.get(cat, 0) + 1

        logger.info(
            "Test Planner: generated %d scenarios — %s",
            scenario_count,
            ", ".join(f"{k}:{v}" for k, v in categories.items()),
        )
        return result
