"""Test Agent Runner — polls server for test jobs, executes Playwright in headed mode.

The runner:
1. Polls the Function App for pending UI test jobs
2. Downloads the generated test plan + scripts
3. Launches Edge in HEADED mode (QA can watch)
4. Executes each test step, reporting status in real-time
5. Captures screenshots at each step
6. Uploads results back to the Function App
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

from .executor import PlaywrightExecutor
from .report import generate_excel_report


class TestAgentRunner:
    def __init__(self, server_url: str, func_key: str):
        self._server = server_url.rstrip("/")
        self._key = func_key
        self._headers = {"Content-Type": "application/json"}

    def _url(self, path: str) -> str:
        sep = "&" if "?" in path else "?"
        return f"{self._server}{path}{sep}code={self._key}"

    def _post_log(self, instance_id: str, level: str, message: str):
        """Send a log entry to the server for the Web UI log panel."""
        try:
            requests.post(
                self._url("/api/agent-log"),
                json={"instance_id": instance_id, "level": level, "message": message},
                timeout=5,
            )
        except Exception:
            pass
        # Also print locally
        colors = {"info": "\033[90m", "run": "\033[33m", "ok": "\033[32m", "err": "\033[31m", "warn": "\033[33m"}
        reset = "\033[0m"
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"{colors.get(level, '')}{ts} [{level.upper()}] {message}{reset}")

    def listen(self, poll_interval: int = 5):
        """Poll the server for pending test jobs."""
        print(f"\n{'='*50}")
        print(f"  BI Test Agent v1.0")
        print(f"  Server: {self._server}")
        print(f"  Polling every {poll_interval}s for test jobs...")
        print(f"{'='*50}\n")

        while True:
            try:
                resp = requests.get(self._url("/api/agent-poll"), timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("job"):
                        job = data["job"]
                        print(f"\n>>> New test job: {job.get('story_id', '')} <<<")
                        self.run_single(job["instance_id"])
                    # else: no job available
            except requests.exceptions.ConnectionError:
                print(f"[WARN] Cannot reach server. Retrying in {poll_interval}s...")
            except Exception as e:
                print(f"[ERROR] {e}")

            time.sleep(poll_interval)

    def run_single(self, instance_id: str):
        """Execute a single test job."""
        self._post_log(instance_id, "info", "Local agent connected. Fetching test plan...")

        # Fetch test details from server
        resp = requests.get(self._url(f"/api/test-progress?instance_id={instance_id}"), timeout=15)
        data = resp.json()

        generated_code = data.get("generated_code")
        app_url = data.get("app_url", "")
        story_id = data.get("story_id", "")
        test_plan = data.get("test_plan", {})

        if not generated_code:
            self._post_log(instance_id, "err", "No generated test code found. Is the pipeline at step 5?")
            return

        scenarios = test_plan.get("test_scenarios", [])
        self._post_log(instance_id, "info", f"Story: {story_id} | App: {app_url}")
        self._post_log(instance_id, "info", f"Test scenarios: {len(scenarios)}")
        self._post_log(instance_id, "run", "Launching Edge browser (headed mode)...")

        # Execute tests with Playwright
        executor = PlaywrightExecutor(app_url=app_url, headed=True)
        results = executor.execute(
            generated_code=generated_code,
            scenarios=scenarios,
            log_callback=lambda level, msg: self._post_log(instance_id, level, msg),
        )

        # Generate Excel report
        report_path = Path(f"test_report_{story_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        generate_excel_report(
            story_id=story_id,
            scenarios=scenarios,
            results=results,
            output_path=report_path,
        )
        self._post_log(instance_id, "ok", f"Excel report saved: {report_path}")

        # Upload results to server
        self._post_log(instance_id, "info", "Uploading results to server...")
        upload_data = {
            "passed": results.get("passed", 0),
            "failed": results.get("failed", 0),
            "errors": results.get("errors", 0),
            "total": results.get("total", 0),
            "test_results": results.get("test_results", []),
            "elapsed_seconds": results.get("elapsed_seconds", 0),
        }

        try:
            resp = requests.post(
                self._url(f"/api/agent-results?instance_id={instance_id}"),
                json=upload_data,
                timeout=15,
            )
            if resp.status_code == 200:
                self._post_log(instance_id, "ok", "Results uploaded. ADO + Teams will be updated.")
            else:
                self._post_log(instance_id, "err", f"Upload failed: {resp.status_code}")
        except Exception as e:
            self._post_log(instance_id, "err", f"Upload error: {e}")

        # Summary
        p, f, e = results.get("passed", 0), results.get("failed", 0), results.get("errors", 0)
        self._post_log(instance_id, "ok" if f == 0 else "err",
                       f"{'='*40}")
        self._post_log(instance_id, "ok" if f == 0 else "err",
                       f"RESULTS: {p} passed, {f} failed, {e} errors ({results.get('elapsed_seconds', 0)}s)")
        self._post_log(instance_id, "info", f"Report: {report_path.absolute()}")
        self._post_log(instance_id, "ok" if f == 0 else "err",
                       f"{'='*40}")
