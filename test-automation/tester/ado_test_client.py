"""ADO Test Management Client — Creates Test Plans, Suites, Cases, and Runs.

Integrates with Azure DevOps Test Management REST API to create the full
test artifact hierarchy linked to user stories.
"""

from __future__ import annotations

import json
import logging
import os
from base64 import b64encode

import requests

logger = logging.getLogger(__name__)

ADO_API_VERSION = "7.1"


class ADOTestClient:
    """Manages ADO Test Plans, Suites, Cases, Runs, and Results."""

    def __init__(self):
        self._org = os.environ.get("ADO_ORG", "")
        self._project = os.environ.get("ADO_PROJECT", "")
        pat = os.environ.get("ADO_PAT", "")
        self._headers = {
            "Authorization": f"Basic {b64encode(f':{pat}'.encode()).decode()}",
            "Content-Type": "application/json",
        }
        self._base = f"https://dev.azure.com/{self._org}/{self._project}/_apis"
        self._test_base = f"https://dev.azure.com/{self._org}/{self._project}/_apis/test"
        self._testplan_base = f"https://dev.azure.com/{self._org}/{self._project}/_apis/testplan"

    def create_test_plan(self, name: str, description: str = "", area_path: str = "") -> dict:
        """Create a Test Plan."""
        url = f"{self._testplan_base}/plans?api-version={ADO_API_VERSION}"
        body = {
            "name": name,
            "description": description or f"Auto-generated test plan: {name}",
        }
        if area_path:
            body["areaPath"] = area_path

        resp = requests.post(url, headers=self._headers, json=body, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        logger.info("Created Test Plan: %s (ID: %s)", name, data.get("id"))
        return {"id": data["id"], "name": data["name"], "url": data.get("url", "")}

    def create_test_suite(self, plan_id: int, name: str, parent_suite_id: int | None = None, requirement_id: int | None = None) -> dict:
        """Create a Test Suite under a Test Plan. Optionally link to a requirement (user story)."""
        # Get the root suite ID if no parent specified
        if not parent_suite_id:
            plan_url = f"{self._testplan_base}/plans/{plan_id}?api-version={ADO_API_VERSION}"
            plan_resp = requests.get(plan_url, headers=self._headers, timeout=15)
            plan_resp.raise_for_status()
            parent_suite_id = plan_resp.json().get("rootSuite", {}).get("id")

        url = f"{self._testplan_base}/plans/{plan_id}/suites?api-version={ADO_API_VERSION}"

        if requirement_id:
            body = {
                "suiteType": "requirementTestSuite",
                "name": name,
                "parentSuite": {"id": parent_suite_id},
                "requirementId": requirement_id,
            }
        else:
            body = {
                "suiteType": "staticTestSuite",
                "name": name,
                "parentSuite": {"id": parent_suite_id},
            }

        resp = requests.post(url, headers=self._headers, json=body, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        logger.info("Created Test Suite: %s (ID: %s)", name, data.get("id"))
        return {"id": data["id"], "name": data.get("name", name)}

    def create_test_case(self, title: str, steps: list[dict], priority: int = 2, area_path: str = "") -> dict:
        """Create a Test Case work item with steps."""
        url = f"{self._base}/wit/workitems/$Test%20Case?api-version={ADO_API_VERSION}"

        # Build steps XML (ADO Test Case steps format)
        steps_xml = "<steps>"
        for i, step in enumerate(steps, 1):
            action = step.get("action", "")
            target = step.get("target", "")
            value = step.get("value", "")
            expected = step.get("expected", "")
            action_text = f"{action} on '{target}'"
            if value:
                action_text += f" with value '{value}'"
            steps_xml += (
                f'<step id="{i}" type="ActionStep">'
                f"<parameterizedString>{action_text}</parameterizedString>"
                f"<parameterizedString>{expected}</parameterizedString>"
                f"</step>"
            )
        steps_xml += "</steps>"

        patches = [
            {"op": "add", "path": "/fields/System.Title", "value": title},
            {"op": "add", "path": "/fields/Microsoft.VSTS.TCM.Steps", "value": steps_xml},
            {"op": "add", "path": "/fields/Microsoft.VSTS.Common.Priority", "value": priority},
        ]
        if area_path:
            patches.append({"op": "add", "path": "/fields/System.AreaPath", "value": area_path})

        resp = requests.post(
            url,
            headers={**self._headers, "Content-Type": "application/json-patch+json"},
            json=patches,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        logger.info("Created Test Case: %s (ID: %s)", title, data.get("id"))
        return {"id": data["id"], "title": title}

    def add_test_cases_to_suite(self, plan_id: int, suite_id: int, test_case_ids: list[int]) -> dict:
        """Add test cases to a test suite."""
        url = f"{self._testplan_base}/plans/{plan_id}/suites/{suite_id}/testcase?api-version={ADO_API_VERSION}"
        body = [{"pointAssignments": [], "workItem": {"id": tc_id}} for tc_id in test_case_ids]
        resp = requests.post(url, headers=self._headers, json=body, timeout=15)
        resp.raise_for_status()
        logger.info("Added %d test cases to suite %s", len(test_case_ids), suite_id)
        return {"added": len(test_case_ids)}

    def create_test_run(self, plan_id: int, suite_id: int, name: str, test_point_ids: list[int] | None = None) -> dict:
        """Create a Test Run."""
        url = f"{self._test_base}/runs?api-version={ADO_API_VERSION}"
        body = {
            "name": name,
            "plan": {"id": plan_id},
            "automated": True,
        }
        if test_point_ids:
            body["pointIds"] = test_point_ids

        resp = requests.post(url, headers=self._headers, json=body, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        logger.info("Created Test Run: %s (ID: %s)", name, data.get("id"))
        return {"id": data["id"], "name": data.get("name", name)}

    def get_test_points(self, plan_id: int, suite_id: int) -> list[dict]:
        """Get test points for a suite (needed to create test runs)."""
        url = f"{self._testplan_base}/plans/{plan_id}/suites/{suite_id}/testpoint?api-version={ADO_API_VERSION}"
        resp = requests.get(url, headers=self._headers, timeout=15)
        resp.raise_for_status()
        return resp.json().get("value", [])

    def update_test_results(self, run_id: int, results: list[dict]) -> dict:
        """Update test results for a test run."""
        url = f"{self._test_base}/runs/{run_id}/results?api-version={ADO_API_VERSION}"
        body = []
        for r in results:
            outcome = "Passed" if r.get("status") == "passed" else "Failed" if r.get("status") == "failed" else "Error"
            entry = {
                "testCase": {"id": r.get("test_case_id")},
                "outcome": outcome,
                "state": "Completed",
                "durationInMs": int(r.get("duration", 0) * 1000),
                "errorMessage": r.get("message", ""),
                "comment": r.get("comment", ""),
            }
            body.append(entry)

        resp = requests.post(url, headers=self._headers, json=body, timeout=15)
        resp.raise_for_status()
        logger.info("Updated %d test results for run %s", len(results), run_id)
        return {"updated": len(results)}

    def complete_test_run(self, run_id: int) -> dict:
        """Mark a test run as completed."""
        url = f"{self._test_base}/runs/{run_id}?api-version={ADO_API_VERSION}"
        body = {"state": "Completed"}
        resp = requests.patch(url, headers=self._headers, json=body, timeout=15)
        resp.raise_for_status()
        return {"status": "completed"}

    def create_bug(self, title: str, repro_steps: str, story_id: int | None = None, priority: int = 2, screenshot_url: str = "") -> dict:
        """Create a Bug work item, optionally linked to the original story."""
        url = f"{self._base}/wit/workitems/$Bug?api-version={ADO_API_VERSION}"
        repro_value = repro_steps
        if screenshot_url:
            repro_value = f"{repro_steps}<br/><img src='{screenshot_url}' />"

        patches = [
            {"op": "add", "path": "/fields/System.Title", "value": title},
            {"op": "add", "path": "/fields/Microsoft.VSTS.TCM.ReproSteps", "value": repro_value},
            {"op": "add", "path": "/fields/Microsoft.VSTS.Common.Priority", "value": priority},
        ]

        if story_id:
            patches.append({
                "op": "add",
                "path": "/relations/-",
                "value": {
                    "rel": "Microsoft.VSTS.Common.TestedBy-Reverse",
                    "url": f"{self._base}/wit/workitems/{story_id}",
                },
            })

        resp = requests.post(
            url,
            headers={**self._headers, "Content-Type": "application/json-patch+json"},
            json=patches,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        logger.info("Created Bug: %s (ID: %s)", title, data.get("id"))
        return {"id": data["id"], "title": title}

    def create_full_test_hierarchy(self, test_plan_data: dict) -> dict:
        """Create complete Test Plan → Suite → Cases from test plan data.
        Returns IDs for all created artifacts."""
        story_id = test_plan_data.get("story_id", "")
        work_item_id = test_plan_data.get("work_item_id")
        plan_name = test_plan_data.get("test_plan_name", f"Tests for {story_id}")
        scenarios = test_plan_data.get("test_scenarios", [])

        # 1. Create Test Plan
        plan = self.create_test_plan(plan_name)
        plan_id = plan["id"]

        # 2. Create Test Suite (linked to story if work_item_id exists)
        suite = self.create_test_suite(
            plan_id=plan_id,
            name=f"Suite — {story_id}",
            requirement_id=work_item_id if work_item_id else None,
        )
        suite_id = suite["id"]

        # 3. Create Test Cases
        test_case_ids = []
        tc_map = {}
        for scenario in scenarios:
            priority_map = {"critical": 1, "high": 2, "medium": 3, "low": 4}
            tc = self.create_test_case(
                title=f"[{scenario.get('id', '')}] {scenario['title']}",
                steps=scenario.get("steps", []),
                priority=priority_map.get(scenario.get("priority", "medium"), 3),
            )
            test_case_ids.append(tc["id"])
            tc_map[scenario.get("id", "")] = tc["id"]

        # 4. Add test cases to suite
        if test_case_ids:
            self.add_test_cases_to_suite(plan_id, suite_id, test_case_ids)

        logger.info(
            "Created full hierarchy: Plan=%s, Suite=%s, %d Cases",
            plan_id, suite_id, len(test_case_ids),
        )

        return {
            "plan_id": plan_id,
            "suite_id": suite_id,
            "test_case_ids": test_case_ids,
            "test_case_map": tc_map,
        }
