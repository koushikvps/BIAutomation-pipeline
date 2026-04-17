"""Azure DevOps REST API client for fetching work items."""

from __future__ import annotations

import base64
import logging
import os
from typing import Optional

import requests

logger = logging.getLogger(__name__)

ADO_API_VERSION = "7.1"


class ADOClient:
    """Fetches work items from Azure DevOps."""

    def __init__(self, org: str = None, project: str = None, pat: str = None):
        self._org = org or os.environ.get("ADO_ORG", "")
        self._project = project or os.environ.get("ADO_PROJECT", "")
        self._pat = pat or os.environ.get("ADO_PAT", "")
        self._base_url = f"https://dev.azure.com/{self._org}/{self._project}/_apis"
        self._headers = {
            "Authorization": f"Basic {base64.b64encode(f':{self._pat}'.encode()).decode()}",
            "Content-Type": "application/json",
        }

    def get_work_item(self, work_item_id: int) -> dict:
        """Fetch a single work item by ID with all fields."""
        url = f"{self._base_url}/wit/workitems/{work_item_id}"
        params = {"$expand": "all", "api-version": ADO_API_VERSION}
        resp = requests.get(url, headers=self._headers, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_work_item_fields(self, work_item_id: int) -> dict:
        """Fetch a work item and return its fields as a flat dict."""
        wi = self.get_work_item(work_item_id)
        fields = wi.get("fields", {})
        return {
            "id": wi.get("id"),
            "title": fields.get("System.Title", ""),
            "description": fields.get("System.Description", ""),
            "acceptance_criteria": fields.get("Microsoft.VSTS.Common.AcceptanceCriteria", ""),
            "state": fields.get("System.State", ""),
            "work_item_type": fields.get("System.WorkItemType", ""),
            "tags": fields.get("System.Tags", ""),
            "assigned_to": fields.get("System.AssignedTo", {}).get("displayName", "") if isinstance(fields.get("System.AssignedTo"), dict) else "",
            "priority": str(fields.get("Microsoft.VSTS.Common.Priority", "2")),
            "area_path": fields.get("System.AreaPath", ""),
            "iteration_path": fields.get("System.IterationPath", ""),
        }

    def update_work_item_state(self, work_item_id: int, state: str, comment: str = None) -> dict:
        """Update work item state and optionally add a comment."""
        url = f"{self._base_url}/wit/workitems/{work_item_id}"
        params = {"api-version": ADO_API_VERSION}
        patch = [{"op": "add", "path": "/fields/System.State", "value": state}]
        if comment:
            patch.append({"op": "add", "path": "/fields/System.History", "value": comment})
        headers = {**self._headers, "Content-Type": "application/json-patch+json"}
        resp = requests.patch(url, headers=headers, params=params, json=patch, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def add_comment(self, work_item_id: int, comment: str) -> dict:
        """Add a comment to a work item."""
        url = f"{self._base_url}/wit/workitems/{work_item_id}/comments"
        params = {"api-version": f"{ADO_API_VERSION}-preview.4"}
        resp = requests.post(url, headers=self._headers, params=params, json={"text": comment}, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def query_work_items(self, wiql: str) -> list[int]:
        """Execute a WIQL query and return work item IDs."""
        url = f"{self._base_url}/wit/wiql"
        params = {"api-version": ADO_API_VERSION}
        resp = requests.post(url, headers=self._headers, params=params, json={"query": wiql}, timeout=30)
        resp.raise_for_status()
        return [item["id"] for item in resp.json().get("workItems", [])]

    def create_work_item(self, title: str, description: str, tags: str = "bi-automation") -> dict:
        """Create a new User Story work item."""
        url = f"{self._base_url}/wit/workitems/$User%20Story"
        params = {"api-version": ADO_API_VERSION}
        patch = [
            {"op": "add", "path": "/fields/System.Title", "value": title},
            {"op": "add", "path": "/fields/System.Description", "value": description},
            {"op": "add", "path": "/fields/System.Tags", "value": tags},
        ]
        headers = {**self._headers, "Content-Type": "application/json-patch+json"}
        resp = requests.post(url, headers=headers, params=params, json=patch, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_tagged_stories(self, tag: str = "bi-automation") -> list[int]:
        """Find User Stories with a specific tag."""
        wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.WorkItemType] = 'User Story' "
            f"AND [System.Tags] CONTAINS '{tag}' "
            f"AND [System.State] <> 'Closed' "
            f"ORDER BY [System.Id] ASC"
        )
        return self.query_work_items(wiql)
