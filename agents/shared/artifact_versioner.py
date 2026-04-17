"""Artifact Versioner: commits generated SQL to Azure DevOps Git repo."""

from __future__ import annotations

import base64
import json
import logging
import os
from typing import Optional

import requests

from .config import AppConfig

logger = logging.getLogger(__name__)

ADO_API_VERSION = "7.1"


class ArtifactVersioner:
    """Commits generated artifacts to ADO Git repository for audit trail."""

    def __init__(self, config: AppConfig):
        self._org = config.ado_org
        self._project = config.ado_project
        self._repo = config.ado_repo
        self._pat = os.environ.get("ADO_PAT", "")
        self._base_url = (
            f"https://dev.azure.com/{self._org}/{self._project}"
            f"/_apis/git/repositories/{self._repo}"
        )
        self._headers = {
            "Authorization": f"Basic {base64.b64encode(f':{self._pat}'.encode()).decode()}",
            "Content-Type": "application/json",
        }

    def commit_artifacts(
        self,
        story_id: str,
        artifacts: list[dict],
        build_plan_json: Optional[str] = None,
        base_branch: str = "fix/bicep-circular-dependency",
    ) -> Optional[dict]:
        """Commit all artifacts to a generated/ branch in the repo.

        Args:
            story_id: e.g. "STORY-515677"
            artifacts: list of {"file_path": str, "content": str}
            build_plan_json: optional build plan to include
            base_branch: branch to base from

        Returns:
            dict with commit_sha and branch, or None on failure
        """
        branch_name = f"generated/{story_id}"

        try:
            # Get latest commit from base branch
            base_ref = self._get_ref(f"refs/heads/{base_branch}")
            if not base_ref:
                logger.warning("Base branch %s not found, skipping versioning", base_branch)
                return None

            old_object_id = base_ref["objectId"]

            # Build list of file changes
            changes = []
            for art in artifacts:
                content_b64 = base64.b64encode(art["content"].encode("utf-8")).decode("utf-8")
                changes.append({
                    "changeType": "add",
                    "item": {"path": f"/generated/{story_id}/{art['file_path']}"},
                    "newContent": {
                        "content": content_b64,
                        "contentType": "base64encoded",
                    },
                })

            if build_plan_json:
                content_b64 = base64.b64encode(build_plan_json.encode("utf-8")).decode("utf-8")
                changes.append({
                    "changeType": "add",
                    "item": {"path": f"/generated/{story_id}/metadata/build_plan.json"},
                    "newContent": {
                        "content": content_b64,
                        "contentType": "base64encoded",
                    },
                })

            if not changes:
                logger.info("No artifacts to commit for %s", story_id)
                return None

            # Create/update branch with commit
            ref_update = self._get_ref(f"refs/heads/{branch_name}")
            if ref_update:
                old_branch_id = ref_update["objectId"]
            else:
                old_branch_id = "0000000000000000000000000000000000000000"

            push_body = {
                "refUpdates": [{
                    "name": f"refs/heads/{branch_name}",
                    "oldObjectId": old_branch_id if ref_update else old_object_id,
                }],
                "commits": [{
                    "comment": f"[BI Automation] Generated artifacts for {story_id}",
                    "changes": changes,
                }],
            }

            # If branch doesn't exist, create from base
            if not ref_update:
                push_body["refUpdates"][0]["oldObjectId"] = old_object_id

            resp = requests.post(
                f"{self._base_url}/pushes?api-version={ADO_API_VERSION}",
                headers=self._headers,
                json=push_body,
                timeout=60,
            )

            if resp.status_code == 409:
                # Branch already has these files, try update
                logger.warning("Conflict pushing to %s, files may already exist", branch_name)
                push_body["commits"][0]["changes"] = [
                    {**c, "changeType": "edit"} for c in changes
                ]
                if ref_update:
                    push_body["refUpdates"][0]["oldObjectId"] = old_branch_id
                resp = requests.post(
                    f"{self._base_url}/pushes?api-version={ADO_API_VERSION}",
                    headers=self._headers,
                    json=push_body,
                    timeout=60,
                )

            if resp.status_code not in (200, 201):
                logger.error("Failed to push artifacts: %s %s", resp.status_code, resp.text[:500])
                return None

            push_data = resp.json()
            commits = push_data.get("commits", [])
            commit_sha = commits[0]["commitId"] if commits else None

            logger.info(
                "Committed %d artifacts for %s to branch %s (sha=%s)",
                len(changes), story_id, branch_name, commit_sha,
            )
            return {"commit_sha": commit_sha, "branch": branch_name}

        except Exception as e:
            logger.error("Artifact versioning failed: %s", e)
            return None

    def _get_ref(self, ref_name: str) -> Optional[dict]:
        """Get a Git ref (branch) from the repo."""
        try:
            resp = requests.get(
                f"{self._base_url}/refs?filter={ref_name.replace('refs/', '')}&api-version={ADO_API_VERSION}",
                headers=self._headers,
                timeout=15,
            )
            if resp.status_code != 200:
                return None
            refs = resp.json().get("value", [])
            for r in refs:
                if r["name"] == ref_name:
                    return r
            return None
        except Exception as e:
            logger.warning("Non-critical error fetching git ref %s: %s", ref_name, e)
            return None
