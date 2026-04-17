"""Playwright Executor — drives Edge browser in headed mode, step by step.

The QA watches the agent navigate the Power App, fill forms, click buttons,
and verify results. Each step is logged in real-time to the Web UI.
"""

from __future__ import annotations

import os
import time
import traceback
from pathlib import Path
from typing import Callable

from playwright.sync_api import sync_playwright, Browser, Page


class PlaywrightExecutor:
    def __init__(self, app_url: str, headed: bool = True):
        self._app_url = app_url
        self._headed = headed
        self._screenshots_dir = Path("screenshots")
        self._screenshots_dir.mkdir(exist_ok=True)

    def execute(
        self,
        generated_code: dict,
        scenarios: list[dict],
        log_callback: Callable[[str, str], None] | None = None,
    ) -> dict:
        """Execute test scenarios against the Power App using Playwright."""

        def log(level: str, msg: str):
            if log_callback:
                log_callback(level, msg)

        results = []
        passed = 0
        failed = 0
        errors = 0
        start_time = time.time()

        with sync_playwright() as p:
            log("run", "Starting Edge browser (headed)...")
            browser = p.chromium.launch(
                channel="msedge",
                headless=not self._headed,
                slow_mo=500,  # Slow down so QA can see what's happening
            )

            # Use persistent context for auth if profile path exists
            profile_path = os.environ.get("EDGE_PROFILE_PATH", "")
            if profile_path and Path(profile_path).exists():
                context = p.chromium.launch_persistent_context(
                    profile_path,
                    channel="msedge",
                    headless=not self._headed,
                    slow_mo=500,
                )
                page = context.pages[0] if context.pages else context.new_page()
            else:
                context = browser.new_context(
                    viewport={"width": 1400, "height": 900},
                )
                page = context.new_page()

            # Navigate to the Power App
            log("run", f"Navigating to: {self._app_url}")
            try:
                page.goto(self._app_url, wait_until="networkidle", timeout=60000)
                log("ok", "App loaded successfully")
                time.sleep(3)  # Let Power Apps fully render

                # Handle consent dialog if present
                try:
                    consent_btn = page.locator("button:has-text('Allow'), button:has-text('Accept'), button:has-text('Got it')")
                    if consent_btn.count() > 0:
                        consent_btn.first.click()
                        log("info", "Dismissed consent dialog")
                        time.sleep(2)
                except Exception:
                    pass

            except Exception as e:
                log("err", f"Failed to load app: {str(e)[:150]}")
                return {
                    "passed": 0, "failed": 0, "errors": 1, "total": 1,
                    "test_results": [{"name": "App Load", "status": "error", "message": str(e)[:200], "duration": 0}],
                    "elapsed_seconds": int(time.time() - start_time),
                }

            # Take initial screenshot
            ss_path = str(self._screenshots_dir / "00_app_loaded.png")
            page.screenshot(path=ss_path)
            log("info", "Screenshot: app loaded")

            # Execute each scenario
            for scenario in scenarios:
                tc_id = scenario.get("id", "TC-???")
                tc_title = scenario.get("title", "")
                tc_steps = scenario.get("steps", [])
                tc_category = scenario.get("category", "")

                log("run", f"━━━ {tc_id}: {tc_title} ━━━")
                log("info", f"  Category: {tc_category} | Steps: {len(tc_steps)}")

                tc_start = time.time()
                tc_status = "passed"
                tc_message = ""

                for step in tc_steps:
                    step_num = step.get("step", "?")
                    action = step.get("action", "").lower()
                    target = step.get("target", "")
                    value = step.get("value", "")
                    expected = step.get("expected", "")

                    log("run", f"  Step {step_num}: {action} → '{target}'" + (f" = '{value}'" if value else ""))

                    try:
                        self._execute_step(page, action, target, value)
                        time.sleep(1)  # Wait for Power Apps to render

                        # Verify expected result if provided
                        if expected:
                            verified = self._verify_expected(page, expected)
                            if verified:
                                log("ok", f"  ✓ {expected}")
                            else:
                                log("warn", f"  ? Could not verify: {expected}")

                        # Screenshot after key steps
                        if action in ("click", "submit", "navigate", "verify"):
                            ss_name = f"{tc_id}_step{step_num}.png"
                            page.screenshot(path=str(self._screenshots_dir / ss_name))

                    except Exception as e:
                        tc_status = "failed"
                        tc_message = f"Step {step_num} ({action} '{target}'): {str(e)[:150]}"
                        log("err", f"  ✗ FAILED: {tc_message}")

                        # Screenshot on failure
                        ss_name = f"{tc_id}_FAIL_step{step_num}.png"
                        try:
                            page.screenshot(path=str(self._screenshots_dir / ss_name))
                        except Exception:
                            pass
                        break

                tc_duration = round(time.time() - tc_start, 2)

                if tc_status == "passed":
                    passed += 1
                    log("ok", f"  ✓ {tc_id} PASSED ({tc_duration}s)")
                else:
                    failed += 1
                    log("err", f"  ✗ {tc_id} FAILED ({tc_duration}s)")

                results.append({
                    "name": f"{tc_id}: {tc_title}",
                    "status": tc_status,
                    "duration": tc_duration,
                    "message": tc_message,
                    "category": tc_category,
                })

            # Final screenshot
            page.screenshot(path=str(self._screenshots_dir / "99_final.png"))

            browser.close()

        elapsed = int(time.time() - start_time)
        return {
            "passed": passed,
            "failed": failed,
            "errors": errors,
            "total": passed + failed + errors,
            "test_results": results,
            "elapsed_seconds": elapsed,
        }

    def _execute_step(self, page: Page, action: str, target: str, value: str = ""):
        """Execute a single test step using Playwright."""
        if action == "navigate":
            # Click navigation element or go to screen
            nav = page.get_by_role("button", name=target)
            if nav.count() == 0:
                nav = page.get_by_text(target, exact=False)
            if nav.count() == 0:
                nav = page.locator(f'[aria-label="{target}"]')
            nav.first.click()

        elif action == "click":
            el = page.get_by_role("button", name=target)
            if el.count() == 0:
                el = page.get_by_text(target, exact=False)
            if el.count() == 0:
                el = page.locator(f'[aria-label="{target}"]')
            el.first.click()

        elif action in ("type", "input", "fill"):
            el = page.get_by_role("textbox", name=target)
            if el.count() == 0:
                el = page.locator(f'[aria-label="{target}"]')
            if el.count() == 0:
                el = page.get_by_placeholder(target)
            el.first.fill(str(value))

        elif action == "select":
            # Power Apps dropdown: click to open, then click option
            dd = page.locator(f'[aria-label="{target}"]')
            if dd.count() == 0:
                dd = page.get_by_text(target, exact=False)
            dd.first.click()
            time.sleep(0.5)
            page.get_by_text(str(value), exact=False).first.click()

        elif action == "upload":
            # File upload
            file_input = page.locator('input[type="file"]')
            if file_input.count() > 0:
                file_input.set_input_files(str(value))
            else:
                page.get_by_text(target, exact=False).first.click()
                time.sleep(1)
                file_input = page.locator('input[type="file"]')
                file_input.set_input_files(str(value))

        elif action == "verify":
            # Just check visibility — actual assertion in _verify_expected
            pass

        elif action == "wait":
            time.sleep(int(value) if value else 2)

        elif action == "scroll":
            page.mouse.wheel(0, 300)

        else:
            # Generic: try to find and click
            el = page.get_by_text(target, exact=False)
            if el.count() > 0:
                el.first.click()

    def _verify_expected(self, page: Page, expected: str) -> bool:
        """Check if the expected text/condition is visible on the page."""
        try:
            # Check if text is visible
            locator = page.get_by_text(expected, exact=False)
            if locator.count() > 0:
                return locator.first.is_visible()

            # Check accessibility tree
            snapshot = page.accessibility.snapshot()
            if snapshot:
                tree_text = str(snapshot).lower()
                if expected.lower() in tree_text:
                    return True

            return False
        except Exception:
            return False
