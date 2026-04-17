"""Teams Incoming Webhook notifications with Adaptive Cards.

Sends rich notifications to a Teams channel at each pipeline stage:
- Pipeline started
- Review gate (with approve/decline links)
- Progress updates
- Pipeline completed
"""

from __future__ import annotations

import logging
import os

import requests

logger = logging.getLogger(__name__)


def _webhook_url() -> str:
    return os.environ.get("TEAMS_WEBHOOK_URL", "")


def _func_base_url() -> str:
    host = os.environ.get("WEBSITE_HOSTNAME", "")
    key = os.environ.get("FUNC_HOST_KEY", "")
    return f"https://{host}", key


def send_card(card: dict) -> dict:
    """Post an adaptive card to Teams via Incoming Webhook."""
    url = _webhook_url()
    if not url:
        return {"status": "skipped", "reason": "TEAMS_WEBHOOK_URL not configured"}

    # Teams webhook expects this wrapper for adaptive cards
    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "contentUrl": None,
                "content": card,
            }
        ],
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            return {"status": "sent"}
        logger.warning("Teams webhook returned %s: %s", resp.status_code, resp.text[:200])
        return {"status": "failed", "code": resp.status_code}
    except Exception as e:
        logger.error("Teams webhook error: %s", e)
        return {"status": "failed", "error": str(e)}


# ─── Card Templates ───────────────────────────────────────────────


def pipeline_started_card(
    story_id: str,
    title: str,
    tables: list[str],
    work_item_id: str = "",
    instance_id: str = "",
) -> dict:
    facts = [
        {"title": "Story ID", "value": story_id},
        {"title": "Tables", "value": ", ".join(tables) if tables else "—"},
    ]
    if work_item_id:
        facts.insert(0, {"title": "Work Item", "value": f"#{work_item_id}"})

    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "msteams": {"width": "Full"},
        "body": [
            {
                "type": "ColumnSet",
                "columns": [
                    {
                        "type": "Column",
                        "width": "auto",
                        "items": [{"type": "TextBlock", "text": "🚀", "size": "ExtraLarge"}],
                    },
                    {
                        "type": "Column",
                        "width": "stretch",
                        "items": [
                            {"type": "TextBlock", "text": "Pipeline Started", "weight": "Bolder", "size": "Large", "color": "Good"},
                            {"type": "TextBlock", "text": title or story_id, "wrap": True, "spacing": "None", "isSubtle": True},
                        ],
                    },
                ],
            },
            {"type": "FactSet", "facts": facts, "separator": True},
            {
                "type": "TextBlock",
                "text": "⬜ Plan → ⬜ Develop → ⬜ ADF → ⬜ Validate → ⬜ Deploy → ⬜ QA",
                "size": "Small",
                "isSubtle": True,
                "wrap": True,
                "spacing": "Medium",
            },
        ],
    }


def review_gate_card(
    instance_id: str,
    mode: str,
    risk_level: str,
    artifact_count: int,
    plan_summary: list[dict],
    title: str = "",
    source_tables: list[str] | None = None,
) -> dict:
    base_url, func_key = _func_base_url()
    approve_url = f"{base_url}/api/approve-test-plan?code={func_key}"
    decline_url = f"{base_url}/api/decline-test-plan?code={func_key}"

    risk_color = "Good" if risk_level == "low" else "Warning" if risk_level == "medium" else "Attention"

    plan_items = []
    for s in plan_summary:
        action_icon = "➕" if s.get("action") == "create" else "🔄"
        layer = s.get("layer", "")
        plan_items.append({
            "type": "TextBlock",
            "text": f"{action_icon} **[{layer}]** {s.get('object_name', '')} ({s.get('artifact_type', '')})",
            "size": "Small",
            "wrap": True,
        })

    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "msteams": {"width": "Full"},
        "body": [
            {
                "type": "ColumnSet",
                "columns": [
                    {"type": "Column", "width": "auto", "items": [{"type": "TextBlock", "text": "⏸️", "size": "ExtraLarge"}]},
                    {
                        "type": "Column",
                        "width": "stretch",
                        "items": [
                            {"type": "TextBlock", "text": "Human Review Required", "weight": "Bolder", "size": "Large", "color": "Warning"},
                            {"type": "TextBlock", "text": title or "Review the execution plan", "spacing": "None", "isSubtle": True, "wrap": True},
                        ],
                    },
                ],
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": "Mode", "value": mode.capitalize()},
                    {"title": "Risk", "value": risk_level.upper()},
                    {"title": "Artifacts", "value": str(artifact_count)},
                ],
                "separator": True,
            },
            {"type": "TextBlock", "text": "**Execution Plan:**", "size": "Small", "spacing": "Medium"},
            *plan_items,
            {"type": "TextBlock", "text": f"Instance: `{instance_id}`", "size": "Small", "isSubtle": True, "spacing": "Medium"},
        ],
        "actions": [
            {
                "type": "Action.Http",
                "title": "✅ Approve & Execute",
                "method": "POST",
                "url": approve_url,
                "body": f'{{"instance_id": "{instance_id}"}}',
                "headers": [{"name": "Content-Type", "value": "application/json"}],
                "style": "positive",
            },
            {
                "type": "Action.Http",
                "title": "❌ Decline",
                "method": "POST",
                "url": decline_url,
                "body": f'{{"instance_id": "{instance_id}", "reason": "Declined via Teams"}}',
                "headers": [{"name": "Content-Type", "value": "application/json"}],
                "style": "destructive",
            },
            {
                "type": "Action.OpenUrl",
                "title": "📊 Open Web UI",
                "url": f"{base_url}/api/ui",
            },
        ],
    }


def progress_card(
    instance_id: str,
    steps: list[dict],
    story_id: str = "",
    elapsed: int = 0,
) -> dict:
    icons = {"completed": "✅", "in_progress": "⏳", "pending": "⬜", "failed": "❌", "escalated": "⚠️"}

    step_lines = []
    for s in steps:
        icon = icons.get(s.get("status", "pending"), "⬜")
        detail = f" — {s['detail']}" if s.get("detail") else ""
        step_lines.append(f"{icon} Step {s['step']}: {s['name']}{detail}")

    completed = len([s for s in steps if s["status"] == "completed"])
    total = len(steps)

    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "msteams": {"width": "Full"},
        "body": [
            {
                "type": "ColumnSet",
                "columns": [
                    {"type": "Column", "width": "auto", "items": [{"type": "TextBlock", "text": "📊", "size": "ExtraLarge"}]},
                    {
                        "type": "Column",
                        "width": "stretch",
                        "items": [
                            {"type": "TextBlock", "text": f"Pipeline Progress — {story_id}", "weight": "Bolder", "size": "Medium"},
                            {"type": "TextBlock", "text": f"{completed}/{total} steps{f' • {elapsed}s' if elapsed else ''}", "spacing": "None", "isSubtle": True},
                        ],
                    },
                ],
            },
            {
                "type": "TextBlock",
                "text": "\n".join(step_lines),
                "wrap": True,
                "size": "Small",
                "separator": True,
                "fontType": "Monospace",
            },
        ],
    }


def completion_card(
    story_id: str,
    title: str,
    deployed: list[str],
    skipped: list[str],
    failed: list[str],
    elapsed: int = 0,
) -> dict:
    is_success = len(failed) == 0 and len(deployed) > 0
    emoji = "✅" if is_success else "⚠️"
    color = "Good" if is_success else "Attention"
    status_text = "Deployment Successful" if is_success else f"Completed with {len(failed)} failure(s)"

    artifact_lines = []
    for d in deployed:
        artifact_lines.append(f"✅ NEW: {d}")
    for s in skipped:
        artifact_lines.append(f"⏭️ EXISTS: {s}")
    for f_item in failed:
        artifact_lines.append(f"❌ FAILED: {f_item}")

    base_url, _ = _func_base_url()

    return {
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
                            {"type": "TextBlock", "text": status_text, "weight": "Bolder", "size": "Large", "color": color},
                            {"type": "TextBlock", "text": f"{title or story_id} — {elapsed}s", "spacing": "None", "isSubtle": True, "wrap": True},
                        ],
                    },
                ],
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": "Story", "value": story_id},
                    {"title": "Deployed", "value": str(len(deployed))},
                    {"title": "Skipped (existing)", "value": str(len(skipped))},
                    {"title": "Failed", "value": str(len(failed))},
                    {"title": "Duration", "value": f"{elapsed}s"},
                ],
                "separator": True,
            },
            {
                "type": "TextBlock",
                "text": "\n".join(artifact_lines) if artifact_lines else "No artifacts",
                "wrap": True,
                "size": "Small",
                "fontType": "Monospace",
                "separator": True,
            },
        ],
        "actions": [
            {
                "type": "Action.OpenUrl",
                "title": "📊 Open Web UI",
                "url": f"{base_url}/api/ui",
            },
        ],
    }
