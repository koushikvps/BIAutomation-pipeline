"""Test Reporter Agent — Reports results to ADO and Teams.

Updates ADO Test Runs with pass/fail results, creates Bug work items
for failures with repro steps and screenshots, and posts Teams cards.
"""

from __future__ import annotations

import logging

from shared.teams_webhook import send_card

logger = logging.getLogger(__name__)


def build_test_results_card(
    story_id: str,
    title: str,
    passed: int,
    failed: int,
    errors: int,
    total: int,
    elapsed: int,
    test_results: list[dict] | None = None,
    bug_ids: list[int] | None = None,
) -> dict:
    """Build Teams adaptive card for test results."""
    is_green = failed == 0 and errors == 0
    emoji = "✅" if is_green else "❌"
    color = "Good" if is_green else "Attention"
    status_text = "All Tests Passed" if is_green else f"{failed + errors} Test(s) Failed"

    result_lines = []
    if test_results:
        for tr in test_results[:15]:
            icon = "✅" if tr["status"] == "passed" else "❌" if tr["status"] == "failed" else "⚠️"
            msg = f" — {tr['message'][:60]}" if tr.get("message") else ""
            result_lines.append(f"{icon} {tr['name']}{msg}")

    bug_text = ""
    if bug_ids:
        bug_text = "Bugs created: " + ", ".join(f"#{b}" for b in bug_ids)

    base_url = ""
    try:
        import os
        host = os.environ.get("WEBSITE_HOSTNAME", "")
        base_url = f"https://{host}"
    except Exception:
        pass

    card = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "msteams": {"width": "Full"},
        "body": [
            {
                "type": "ColumnSet",
                "columns": [
                    {"type": "Column", "width": "auto", "items": [{"type": "TextBlock", "text": emoji, "size": "ExtraLarge"}]},
                    {
                        "type": "Column",
                        "width": "stretch",
                        "items": [
                            {"type": "TextBlock", "text": f"Test Results: {status_text}", "weight": "Bolder", "size": "Large", "color": color},
                            {"type": "TextBlock", "text": f"{title or story_id} — {elapsed}s", "spacing": "None", "isSubtle": True, "wrap": True},
                        ],
                    },
                ],
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": "Story", "value": story_id},
                    {"title": "Total", "value": str(total)},
                    {"title": "Passed", "value": str(passed)},
                    {"title": "Failed", "value": str(failed)},
                    {"title": "Errors", "value": str(errors)},
                    {"title": "Duration", "value": f"{elapsed}s"},
                ],
                "separator": True,
            },
        ],
    }

    if result_lines:
        card["body"].append({
            "type": "TextBlock",
            "text": "\n".join(result_lines),
            "wrap": True,
            "size": "Small",
            "fontType": "Monospace",
            "separator": True,
        })

    if bug_text:
        card["body"].append({
            "type": "TextBlock",
            "text": bug_text,
            "wrap": True,
            "size": "Small",
            "color": "Attention",
            "weight": "Bolder",
        })

    if base_url:
        card["actions"] = [
            {"type": "Action.OpenUrl", "title": "📊 View in Web UI", "url": f"{base_url}/api/ui"},
        ]

    return card


def report_results(
    story_id: str,
    title: str,
    results: dict,
    bug_ids: list[int] | None = None,
) -> dict:
    """Send test results to Teams."""
    card = build_test_results_card(
        story_id=story_id,
        title=title,
        passed=results.get("passed", 0),
        failed=results.get("failed", 0),
        errors=results.get("errors", 0),
        total=results.get("total", 0),
        elapsed=results.get("elapsed_seconds", 0),
        test_results=results.get("test_results"),
        bug_ids=bug_ids,
    )
    return send_card(card)
