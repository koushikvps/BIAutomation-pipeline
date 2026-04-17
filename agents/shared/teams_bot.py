"""Microsoft Teams Bot using Bot Framework SDK.

Handles conversational interactions, adaptive cards for review gates,
and proactive notifications for pipeline status updates.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

import requests
from botbuilder.core import (
    ActivityHandler,
    CardFactory,
    MessageFactory,
    TurnContext,
)
from botbuilder.schema import (
    Activity,
    ActivityTypes,
    Attachment,
    ConversationReference,
    HeroCard,
    CardAction,
    ActionTypes,
)

logger = logging.getLogger(__name__)

# In-memory store for conversation references (for proactive messaging)
CONVERSATION_REFERENCES: dict[str, ConversationReference] = {}

# In-memory store for active pipeline tracking per user
USER_PIPELINES: dict[str, dict] = {}


def _func_url() -> str:
    host = os.environ.get("WEBSITE_HOSTNAME", "localhost")
    return f"https://{host}"


def _func_key() -> str:
    return os.environ.get("FUNC_HOST_KEY", "")


# ─── Adaptive Card Builders ───────────────────────────────────────


def welcome_card() -> Attachment:
    """Welcome card shown when bot is installed or user says hello."""
    card = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [
            {
                "type": "ColumnSet",
                "columns": [
                    {
                        "type": "Column",
                        "width": "auto",
                        "items": [{"type": "Image", "url": "https://img.icons8.com/fluency/48/bot.png", "size": "Small"}],
                    },
                    {
                        "type": "Column",
                        "width": "stretch",
                        "items": [
                            {"type": "TextBlock", "text": "BI Automation Platform", "weight": "Bolder", "size": "Large", "color": "Accent"},
                            {"type": "TextBlock", "text": "AI-powered medallion architecture builder", "spacing": "None", "isSubtle": True},
                        ],
                    },
                ],
            },
            {"type": "TextBlock", "text": "I can build your Synapse data models automatically from Azure DevOps stories or plain English descriptions.", "wrap": True, "spacing": "Medium"},
            {
                "type": "ActionSet",
                "actions": [
                    {"type": "Action.Submit", "title": "Build from ADO Work Item", "data": {"action": "show_ado_form"}},
                    {"type": "Action.Submit", "title": "Build from Description", "data": {"action": "show_freetext_form"}},
                    {"type": "Action.Submit", "title": "Check Pipeline Status", "data": {"action": "show_status_form"}},
                ],
            },
        ],
    }
    return CardFactory.adaptive_card(card)


def ado_input_card() -> Attachment:
    """Card with input field for ADO work item ID."""
    card = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [
            {"type": "TextBlock", "text": "Build from ADO Work Item", "weight": "Bolder", "size": "Medium"},
            {"type": "TextBlock", "text": "Enter the work item ID to fetch the story and build the medallion architecture.", "wrap": True, "isSubtle": True},
            {"type": "Input.Number", "id": "work_item_id", "placeholder": "e.g. 515677", "label": "Work Item ID", "isRequired": True},
        ],
        "actions": [
            {"type": "Action.Submit", "title": "Start Pipeline", "data": {"action": "start_ado_pipeline"}, "style": "positive"},
        ],
    }
    return CardFactory.adaptive_card(card)


def freetext_input_card() -> Attachment:
    """Card with text area for free-text story description."""
    card = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [
            {"type": "TextBlock", "text": "Build from Description", "weight": "Bolder", "size": "Medium"},
            {"type": "TextBlock", "text": "Describe what you want to build. I'll interpret it and generate the data model.", "wrap": True, "isSubtle": True},
            {"type": "Input.Text", "id": "story_text", "placeholder": "e.g. Create a daily sales summary by region and product category from the sales tables", "label": "Story Description", "isMultiline": True, "isRequired": True},
        ],
        "actions": [
            {"type": "Action.Submit", "title": "Start Pipeline", "data": {"action": "start_freetext_pipeline"}, "style": "positive"},
        ],
    }
    return CardFactory.adaptive_card(card)


def pipeline_started_card(title: str, story_id: str, tables: list[str], instance_id: str, work_item_id: str = "") -> Attachment:
    """Card shown when pipeline starts."""
    facts = [
        {"title": "Story", "value": title},
        {"title": "Story ID", "value": story_id},
        {"title": "Tables", "value": ", ".join(tables)},
        {"title": "Instance", "value": instance_id[:12] + "..."},
    ]
    if work_item_id:
        facts.insert(0, {"title": "Work Item", "value": f"#{work_item_id}"})

    card = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [
            {
                "type": "ColumnSet",
                "columns": [
                    {"type": "Column", "width": "auto", "items": [{"type": "TextBlock", "text": "🚀", "size": "Large"}]},
                    {
                        "type": "Column",
                        "width": "stretch",
                        "items": [
                            {"type": "TextBlock", "text": "Pipeline Started", "weight": "Bolder", "size": "Medium", "color": "Good"},
                            {"type": "TextBlock", "text": "Your BI model is being built...", "spacing": "None", "isSubtle": True},
                        ],
                    },
                ],
            },
            {"type": "FactSet", "facts": facts},
            {
                "type": "ColumnSet",
                "columns": [
                    {"type": "Column", "width": "stretch", "items": [
                        {"type": "TextBlock", "text": "⬜ Planner → ⬜ Developer → ⬜ ADF → ⬜ Validate → ⬜ Deploy → ⬜ QA", "size": "Small", "isSubtle": True, "wrap": True},
                    ]},
                ],
            },
        ],
        "actions": [
            {"type": "Action.Submit", "title": "Check Progress", "data": {"action": "check_progress", "instance_id": instance_id}},
        ],
    }
    return CardFactory.adaptive_card(card)


def review_card(instance_id: str, review_data: dict) -> Attachment:
    """Adaptive card for human review gate — approve or decline the plan."""
    mode = review_data.get("mode", "unknown")
    risk = review_data.get("risk_level", "unknown")
    artifacts = review_data.get("artifact_count", 0)
    title = review_data.get("title", "")
    plan_summary = review_data.get("plan_summary", [])
    validations = review_data.get("validation_requirements", [])

    risk_color = "Good" if risk == "low" else "Warning" if risk == "medium" else "Attention"

    plan_items = []
    for s in plan_summary:
        action_icon = "➕" if s.get("action") == "create" else "🔄"
        plan_items.append({
            "type": "ColumnSet",
            "columns": [
                {"type": "Column", "width": "auto", "items": [{"type": "TextBlock", "text": action_icon, "size": "Small"}]},
                {"type": "Column", "width": "auto", "items": [{"type": "TextBlock", "text": s.get("layer", ""), "weight": "Bolder", "size": "Small", "color": "Accent"}]},
                {"type": "Column", "width": "stretch", "items": [{"type": "TextBlock", "text": f"{s.get('object_name', '')} ({s.get('artifact_type', '')})", "size": "Small", "wrap": True}]},
            ],
        })

    validation_text = ", ".join(f"✅ {v.get('check_type', '')}" for v in validations) if validations else "Standard checks"

    card = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [
            {
                "type": "ColumnSet",
                "columns": [
                    {"type": "Column", "width": "auto", "items": [{"type": "TextBlock", "text": "⏸️", "size": "Large"}]},
                    {
                        "type": "Column",
                        "width": "stretch",
                        "items": [
                            {"type": "TextBlock", "text": "Human Review Required", "weight": "Bolder", "size": "Medium", "color": "Warning"},
                            {"type": "TextBlock", "text": title or "Review the execution plan before proceeding", "spacing": "None", "isSubtle": True, "wrap": True},
                        ],
                    },
                ],
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": "Mode", "value": mode.capitalize()},
                    {"title": "Risk Level", "value": risk.upper()},
                    {"title": "Artifacts", "value": str(artifacts)},
                    {"title": "Validations", "value": validation_text},
                ],
            },
            {"type": "TextBlock", "text": "Execution Plan", "weight": "Bolder", "size": "Small", "spacing": "Medium"},
            *plan_items,
        ],
        "actions": [
            {"type": "Action.Submit", "title": "✅ Approve & Execute", "data": {"action": "approve_plan", "instance_id": instance_id}, "style": "positive"},
            {"type": "Action.Submit", "title": "❌ Decline", "data": {"action": "decline_plan", "instance_id": instance_id}, "style": "destructive"},
        ],
    }
    return CardFactory.adaptive_card(card)


def progress_card(instance_id: str, steps: list[dict], story_id: str = "", elapsed: int = 0) -> Attachment:
    """Card showing pipeline progress with step status."""
    icons = {"completed": "✅", "in_progress": "⏳", "pending": "⬜", "failed": "❌", "escalated": "⚠️"}

    step_items = []
    for s in steps:
        icon = icons.get(s.get("status", "pending"), "⬜")
        detail = f" — {s['detail']}" if s.get("detail") else ""
        color = "Good" if s["status"] == "completed" else "Warning" if s["status"] == "in_progress" else "Default"
        step_items.append({
            "type": "TextBlock",
            "text": f"{icon} **Step {s['step']}**: {s['name']}{detail}",
            "size": "Small",
            "wrap": True,
            "color": color,
        })

    completed = len([s for s in steps if s["status"] == "completed"])
    total = len(steps)

    card = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [
            {
                "type": "ColumnSet",
                "columns": [
                    {"type": "Column", "width": "auto", "items": [{"type": "TextBlock", "text": "📊", "size": "Large"}]},
                    {
                        "type": "Column",
                        "width": "stretch",
                        "items": [
                            {"type": "TextBlock", "text": f"Pipeline Progress — {story_id}", "weight": "Bolder", "size": "Medium"},
                            {"type": "TextBlock", "text": f"{completed}/{total} steps complete{f' ({elapsed}s)' if elapsed else ''}", "spacing": "None", "isSubtle": True},
                        ],
                    },
                ],
            },
            *step_items,
        ],
        "actions": [
            {"type": "Action.Submit", "title": "Refresh", "data": {"action": "check_progress", "instance_id": instance_id}},
        ],
    }
    return CardFactory.adaptive_card(card)


def completion_card(story_id: str, status: str, deployed: list[str], skipped: list[str], failed: list[str], elapsed: int = 0) -> Attachment:
    """Card shown when pipeline completes."""
    is_success = len(failed) == 0 and len(deployed) > 0
    emoji = "✅" if is_success else "⚠️" if failed else "ℹ️"
    color = "Good" if is_success else "Attention"
    status_text = "Successfully Deployed" if is_success else f"Completed with {len(failed)} failure(s)"

    artifact_items = []
    for d in deployed:
        artifact_items.append({"type": "TextBlock", "text": f"✅ NEW: {d}", "size": "Small", "color": "Good"})
    for s in skipped:
        artifact_items.append({"type": "TextBlock", "text": f"⏭️ EXISTS: {s}", "size": "Small", "isSubtle": True})
    for f_item in failed:
        artifact_items.append({"type": "TextBlock", "text": f"❌ FAILED: {f_item}", "size": "Small", "color": "Attention"})

    card = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [
            {
                "type": "ColumnSet",
                "columns": [
                    {"type": "Column", "width": "auto", "items": [{"type": "TextBlock", "text": emoji, "size": "Large"}]},
                    {
                        "type": "Column",
                        "width": "stretch",
                        "items": [
                            {"type": "TextBlock", "text": f"Pipeline {status_text}", "weight": "Bolder", "size": "Medium", "color": color},
                            {"type": "TextBlock", "text": f"{story_id} — {elapsed}s", "spacing": "None", "isSubtle": True},
                        ],
                    },
                ],
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": "Deployed", "value": str(len(deployed))},
                    {"title": "Skipped", "value": str(len(skipped))},
                    {"title": "Failed", "value": str(len(failed))},
                ],
            },
            *artifact_items,
        ],
        "actions": [
            {"type": "Action.Submit", "title": "Build Another", "data": {"action": "show_ado_form"}},
        ],
    }
    return CardFactory.adaptive_card(card)


# ─── Bot Activity Handler ─────────────────────────────────────────


class BIAutomationBot(ActivityHandler):
    """Handles Teams messages, card actions, and proactive notifications."""

    async def on_members_added_activity(self, members_added, turn_context: TurnContext):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity(
                    MessageFactory.attachment(welcome_card())
                )

    async def on_message_activity(self, turn_context: TurnContext):
        # Save conversation reference for proactive messaging
        _save_conversation_reference(turn_context.activity)

        # Handle adaptive card submit actions
        if turn_context.activity.value:
            await self._handle_card_action(turn_context)
            return

        text = (turn_context.activity.text or "").strip()
        # Strip bot mention
        text = re.sub(r'<at>[^<]+</at>\s*', '', text).strip()

        if not text:
            await turn_context.send_activity(MessageFactory.attachment(welcome_card()))
            return

        # Natural language command parsing
        text_lower = text.lower()

        # Greeting
        if text_lower in ("hello", "hi", "hey", "help", "start", "menu"):
            await turn_context.send_activity(MessageFactory.attachment(welcome_card()))
            return

        # Build from ADO: "build 515677" or "process WI-515677"
        match = re.match(r'(?:process|run|build|execute)\s+(?:WI[- ]?|STORY[- ]?|#)?(\d+)', text, re.IGNORECASE)
        if match:
            work_item_id = match.group(1)
            await self._start_ado_pipeline(turn_context, work_item_id)
            return

        # Status check: "status abc123..."
        match = re.match(r'status\s+([a-f0-9]+)', text, re.IGNORECASE)
        if match:
            instance_id = match.group(1)
            await self._check_progress(turn_context, instance_id)
            return

        # List stories
        if re.match(r'list|find|show', text_lower):
            await self._list_stories(turn_context)
            return

        # Try to interpret as a work item ID (just a number)
        if re.match(r'^\d{4,}$', text):
            await self._start_ado_pipeline(turn_context, text)
            return

        # Fallback: treat as free-text story
        if len(text) > 20:
            await self._start_freetext_pipeline(turn_context, text)
            return

        await turn_context.send_activity(MessageFactory.attachment(welcome_card()))

    async def _handle_card_action(self, turn_context: TurnContext):
        """Handle adaptive card submit button clicks."""
        data = turn_context.activity.value or {}
        action = data.get("action", "")

        if action == "show_ado_form":
            await turn_context.send_activity(MessageFactory.attachment(ado_input_card()))

        elif action == "show_freetext_form":
            await turn_context.send_activity(MessageFactory.attachment(freetext_input_card()))

        elif action == "show_status_form":
            await turn_context.send_activity(
                MessageFactory.text("Send me a work item ID or instance ID to check status.\n\nExample: `status 515677`")
            )

        elif action == "start_ado_pipeline":
            wid = str(data.get("work_item_id", "")).strip()
            if not wid:
                await turn_context.send_activity(MessageFactory.text("Please enter a valid work item ID."))
                return
            await self._start_ado_pipeline(turn_context, wid)

        elif action == "start_freetext_pipeline":
            text = data.get("story_text", "").strip()
            if not text:
                await turn_context.send_activity(MessageFactory.text("Please enter a story description."))
                return
            await self._start_freetext_pipeline(turn_context, text)

        elif action == "approve_plan":
            instance_id = data.get("instance_id", "")
            await self._approve_plan(turn_context, instance_id)

        elif action == "decline_plan":
            instance_id = data.get("instance_id", "")
            await self._decline_plan(turn_context, instance_id)

        elif action == "check_progress":
            instance_id = data.get("instance_id", "")
            await self._check_progress(turn_context, instance_id)

        else:
            await turn_context.send_activity(MessageFactory.attachment(welcome_card()))

    async def _start_ado_pipeline(self, turn_context: TurnContext, work_item_id: str):
        """Start pipeline from ADO work item."""
        await turn_context.send_activity(
            MessageFactory.text(f"⏳ Fetching work item **#{work_item_id}** from Azure DevOps...")
        )

        try:
            url = f"{_func_url()}/api/process-ado-story?code={_func_key()}&wait=false"
            resp = requests.post(url, json={"work_item_id": work_item_id}, timeout=30)
            data = resp.json()

            if resp.status_code != 200 or data.get("error"):
                await turn_context.send_activity(
                    MessageFactory.text(f"❌ Error: {data.get('error', 'Unknown error')}")
                )
                return

            instance_id = data.get("instance_id", "")
            story_id = data.get("story_id", "")
            tables = data.get("source_tables", [])
            title = data.get("title", "")

            # Track pipeline for this user
            user_id = turn_context.activity.from_property.id
            USER_PIPELINES[user_id] = {
                "instance_id": instance_id,
                "story_id": story_id,
                "work_item_id": work_item_id,
                "start_time": time.time(),
            }

            await turn_context.send_activity(
                MessageFactory.attachment(pipeline_started_card(title, story_id, tables, instance_id, work_item_id))
            )

            # Start background polling for this pipeline
            _save_conversation_reference(turn_context.activity)

        except Exception as e:
            logger.error("Error starting ADO pipeline: %s", e)
            await turn_context.send_activity(MessageFactory.text(f"❌ Error: {str(e)}"))

    async def _start_freetext_pipeline(self, turn_context: TurnContext, text: str):
        """Start pipeline from free-text description."""
        await turn_context.send_activity(
            MessageFactory.text("⏳ Interpreting your story and starting the pipeline...")
        )

        try:
            url = f"{_func_url()}/api/process-free-story?code={_func_key()}"
            resp = requests.post(url, json={"text": text, "title": text[:80]}, timeout=60)
            data = resp.json()

            if resp.status_code != 200 or data.get("error"):
                error_msg = data.get("error", "Unknown error")
                hint = data.get("hint", "")
                await turn_context.send_activity(
                    MessageFactory.text(f"❌ Error: {error_msg}{f' — {hint}' if hint else ''}")
                )
                return

            instance_id = data.get("instance_id", "")
            story_id = data.get("story_id", "")
            tables = data.get("source_tables", [])
            title = data.get("title", text[:60])

            user_id = turn_context.activity.from_property.id
            USER_PIPELINES[user_id] = {
                "instance_id": instance_id,
                "story_id": story_id,
                "start_time": time.time(),
            }

            await turn_context.send_activity(
                MessageFactory.attachment(pipeline_started_card(title, story_id, tables, instance_id))
            )

            _save_conversation_reference(turn_context.activity)

        except Exception as e:
            logger.error("Error starting free-text pipeline: %s", e)
            await turn_context.send_activity(MessageFactory.text(f"❌ Error: {str(e)}"))

    async def _approve_plan(self, turn_context: TurnContext, instance_id: str):
        """Approve the execution plan."""
        try:
            url = f"{_func_url()}/api/approve-plan?code={_func_key()}"
            resp = requests.post(url, json={"instance_id": instance_id}, timeout=15)
            data = resp.json()

            if data.get("status") == "approved":
                await turn_context.send_activity(
                    MessageFactory.text("✅ **Plan approved!** Pipeline is resuming — Developer agent is generating SQL...")
                )
            else:
                await turn_context.send_activity(
                    MessageFactory.text(f"⚠️ Could not approve: {data.get('error', 'unknown')}")
                )
        except Exception as e:
            await turn_context.send_activity(MessageFactory.text(f"❌ Error approving: {str(e)}"))

    async def _decline_plan(self, turn_context: TurnContext, instance_id: str):
        """Decline the execution plan."""
        try:
            url = f"{_func_url()}/api/decline-plan?code={_func_key()}"
            resp = requests.post(url, json={"instance_id": instance_id, "reason": "Declined via Teams"}, timeout=15)
            data = resp.json()

            if data.get("status") == "declined":
                await turn_context.send_activity(
                    MessageFactory.text("❌ **Plan declined.** Pipeline has been stopped.")
                )
            else:
                await turn_context.send_activity(
                    MessageFactory.text(f"⚠️ Could not decline: {data.get('error', 'unknown')}")
                )
        except Exception as e:
            await turn_context.send_activity(MessageFactory.text(f"❌ Error declining: {str(e)}"))

    async def _check_progress(self, turn_context: TurnContext, instance_id: str):
        """Check pipeline progress and send adaptive card."""
        try:
            url = f"{_func_url()}/api/pipeline-progress?instance_id={instance_id}&code={_func_key()}"
            resp = requests.get(url, timeout=15)
            data = resp.json()

            if data.get("error"):
                await turn_context.send_activity(
                    MessageFactory.text(f"⚠️ {data['error']}")
                )
                return

            steps = data.get("steps", [])
            story_id = data.get("steps", [{}])[0].get("detail", "") if steps else ""

            # Check if awaiting review
            if data.get("awaiting_approval"):
                review = data.get("review", {})
                await turn_context.send_activity(
                    MessageFactory.attachment(review_card(instance_id, review))
                )
                return

            # Check if complete
            if data.get("is_complete"):
                progress_text = data.get("progress_text", "")
                deployed = re.findall(r'NEW: (.+)', progress_text)
                skipped = re.findall(r'EXISTS: (.+)', progress_text)
                failed_items = re.findall(r'FAILED: (.+)', progress_text)

                user_id = turn_context.activity.from_property.id
                pipeline_info = USER_PIPELINES.get(user_id, {})
                elapsed = int(time.time() - pipeline_info.get("start_time", time.time()))
                sid = pipeline_info.get("story_id", "")

                await turn_context.send_activity(
                    MessageFactory.attachment(completion_card(sid, "completed", deployed, skipped, failed_items, elapsed))
                )
                return

            # Show progress
            await turn_context.send_activity(
                MessageFactory.attachment(progress_card(instance_id, steps, data.get("story_id", "")))
            )

        except Exception as e:
            logger.error("Error checking progress: %s", e)
            await turn_context.send_activity(MessageFactory.text(f"❌ Error: {str(e)}"))

    async def _list_stories(self, turn_context: TurnContext):
        """List ADO stories tagged for BI automation."""
        try:
            from shared.ado_client import ADOClient
            ado = ADOClient()
            story_ids = ado.get_tagged_stories("bi-automation")
            if not story_ids:
                await turn_context.send_activity(
                    MessageFactory.text("No stories found with tag `bi-automation`. Tag your ADO user stories to use them here.")
                )
                return

            items = "\n".join(f"- **WI-{sid}**" for sid in story_ids[:10])
            await turn_context.send_activity(
                MessageFactory.text(f"Found {len(story_ids)} stories tagged `bi-automation`:\n\n{items}\n\nType `build <id>` to start a pipeline.")
            )
        except Exception as e:
            await turn_context.send_activity(MessageFactory.text(f"❌ Error listing stories: {str(e)}"))


def _save_conversation_reference(activity: Activity):
    """Save conversation reference for proactive messaging later."""
    ref = TurnContext.get_conversation_reference(activity)
    user_id = activity.from_property.id if activity.from_property else "unknown"
    CONVERSATION_REFERENCES[user_id] = ref


def get_conversation_reference(user_id: str) -> ConversationReference | None:
    return CONVERSATION_REFERENCES.get(user_id)
