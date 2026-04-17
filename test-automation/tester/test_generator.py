"""Test Generator Agent — AI generates Playwright Python test scripts.

Takes structured test scenarios from the Test Planner and generates executable
Playwright scripts targeting Power Apps Canvas in Microsoft Edge.
"""

from __future__ import annotations

import json
import logging

from shared.config import AppConfig
from shared.llm_client import LLMClient

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are an expert Playwright test automation engineer for Power Apps Canvas applications.

Generate Python Playwright test code for Microsoft Edge that tests a Power Apps Canvas app.

CRITICAL Power Apps Canvas patterns:
1. Authentication: Edge profile is pre-authenticated — skip login
2. App loading: Wait for the canvas to fully load with `page.wait_for_selector('[data-control-name]', timeout=30000)` or `page.wait_for_load_state('networkidle')`
3. Controls: Use these selector strategies IN ORDER of preference:
   - `page.get_by_role("button", name="Submit")` for buttons
   - `page.get_by_role("textbox", name="Amount")` for text inputs
   - `page.get_by_text("Expense Report")` for labels/text
   - `page.locator('[aria-label="Amount"]')` for aria-labeled controls
   - `page.locator('[data-control-name="txtAmount"]')` as last resort
4. Dropdowns: Power Apps uses custom dropdowns — click to open, then click option by text
5. Date pickers: Type the date value directly into the input
6. Galleries/Lists: Use `page.locator('[aria-label="Gallery"] >> nth=0')` for items
7. File upload: Use `page.set_input_files()` on hidden file input after clicking upload button
8. Navigation: Click navigation icons/buttons, don't use URL changes
9. Assertions: Check for success/error banners, control values, visibility

IMPORTANT:
- Each test function must be independent
- Use fixtures for common setup (app navigation, test data)
- Take screenshot on failure: `page.screenshot(path=f"screenshots/{test_name}_failure.png")`
- Take screenshot on key verification points
- Add explicit waits after clicks (Power Apps renders async)
- Handle the "Allow" consent dialog if it appears

Return a JSON object with:
{
  "test_files": [
    {
      "filename": "test_<story_id>.py",
      "content": "<full python test code>"
    }
  ],
  "conftest": "<conftest.py content with fixtures>",
  "page_objects": [
    {
      "filename": "pages/<screen_name>.py",
      "content": "<page object class>"
    }
  ]
}
"""


class TestGeneratorAgent:
    def __init__(self, config: AppConfig):
        self._llm = LLMClient(config)

    def generate_tests(self, test_plan: dict) -> dict:
        """Generate Playwright test scripts from test plan."""
        story_id = test_plan.get("story_id", "unknown")
        app_url = test_plan.get("app_url", "")
        scenarios = test_plan.get("test_scenarios", [])
        screens = test_plan.get("app_screens", [])
        test_data_req = test_plan.get("test_data_requirements", {})

        user_prompt = f"""Generate Playwright Python tests for these scenarios:

App URL: {app_url}
App Screens: {', '.join(screens)}
Story: {story_id}

Test Scenarios:
{json.dumps(scenarios, indent=2)}

Test Data Requirements:
{json.dumps(test_data_req, indent=2)}

Requirements:
- Use Microsoft Edge (channel="msedge") with persistent context (pre-authenticated profile)
- Use pytest framework with async support (pytest-playwright)
- Generate a conftest.py with:
  - Edge browser fixture with persistent profile
  - App URL fixture
  - Screenshot-on-failure fixture
  - Test data fixture that reads from Excel/dict
- Generate page object classes for each screen
- Generate test file with all scenarios as separate test functions
- Each test takes a screenshot at the final assertion point
- Name screenshots: screenshots/<test_id>_<step>.png
"""

        logger.info("Test Generator: creating Playwright scripts for %d scenarios", len(scenarios))
        result = self._llm.chat_json(
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user_prompt,
            temperature=0.1,
            max_tokens=8192,
        )

        # Count generated artifacts
        test_files = result.get("test_files", [])
        page_objects = result.get("page_objects", [])
        has_conftest = bool(result.get("conftest"))

        logger.info(
            "Test Generator: produced %d test files, %d page objects, conftest=%s",
            len(test_files), len(page_objects), has_conftest,
        )
        return result
