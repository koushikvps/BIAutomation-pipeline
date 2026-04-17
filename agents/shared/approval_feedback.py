"""Approval Feedback Loop: indexes human-approved plans back into the RAG knowledge base.

When the Human Review Gate approves (or modifies) a plan, the approved output
becomes grounding data for future stories. This creates a virtuous cycle:
  Story 1: LLM uses templates → Human approves → approved plan indexed
  Story 2: LLM uses templates + Story 1's approved artifacts → better output
  Story N: Knowledge base rich with client-specific approved patterns

Also captures:
  - Declined plans (as negative examples / anti-patterns)
  - Validator findings (approved corrections feed back)
  - Human edits (diffs between proposed and approved)
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from typing import Optional

from .rag_retriever import RAGRetriever, RAGDocument, DocumentType

logger = logging.getLogger(__name__)


class ApprovalFeedbackLoop:
    """Indexes approved pipeline artifacts back into the RAG knowledge base."""

    def __init__(self, retriever: Optional[RAGRetriever] = None):
        self._retriever = retriever or RAGRetriever()

    def on_plan_approved(self, story_id: str, plan: dict, instance_id: str = "") -> int:
        """Index an approved Planner output into the knowledge base."""
        docs = []

        tables_used = plan.get("tables", plan.get("source_tables", []))
        if isinstance(tables_used, list):
            for t in tables_used:
                if isinstance(t, dict):
                    name = t.get("name", t.get("table", ""))
                    schema = t.get("schema", "")
                    cols = t.get("columns", [])
                    if name:
                        col_text = ", ".join(c if isinstance(c, str) else c.get("name", "") for c in cols)
                        docs.append(RAGDocument(
                            doc_id=f"approved:table:{story_id}:{schema}.{name}",
                            doc_type=DocumentType.TABLE_SCHEMA,
                            content=f"APPROVED [{schema}].[{name}] (used in story {story_id})\nColumns: {col_text}",
                            metadata={"story_id": story_id, "origin": "approved_plan", "approved_at": datetime.utcnow().isoformat()},
                        ))

        joins = plan.get("joins", plan.get("join_conditions", []))
        if isinstance(joins, list):
            for j in joins:
                if isinstance(j, str):
                    doc_id_hash = hashlib.md5(f"{story_id}:{j}".encode()).hexdigest()[:8]
                    docs.append(RAGDocument(
                        doc_id=f"approved:join:{doc_id_hash}",
                        doc_type=DocumentType.APPROVED_JOIN,
                        content=f"APPROVED JOIN (story {story_id}): {j}",
                        metadata={"story_id": story_id, "origin": "approved_plan"},
                    ))
                elif isinstance(j, dict):
                    left = j.get("left", "")
                    right = j.get("right", "")
                    jtype = j.get("type", "INNER")
                    docs.append(RAGDocument(
                        doc_id=f"approved:join:{story_id}:{left}-{right}",
                        doc_type=DocumentType.APPROVED_JOIN,
                        content=f"APPROVED JOIN (story {story_id}): {left} {jtype} JOIN {right}",
                        metadata={"story_id": story_id, "origin": "approved_plan"},
                    ))

        sql_code = plan.get("sql", plan.get("code", plan.get("generated_sql", "")))
        if sql_code and len(sql_code) > 20:
            docs.append(RAGDocument(
                doc_id=f"approved:sql:{story_id}",
                doc_type=DocumentType.SQL_PATTERN,
                content=f"APPROVED SQL (story {story_id}):\n{sql_code[:3000]}",
                metadata={"story_id": story_id, "origin": "approved_plan"},
            ))

        if docs:
            self._retriever.index_documents(docs)
            logger.info("Approval feedback: indexed %d documents from story %s", len(docs), story_id)
        return len(docs)

    def on_plan_declined(self, story_id: str, plan: dict, reason: str = "") -> int:
        """Record a declined plan as negative signal (not used as grounding, but logged)."""
        docs = []
        sql_code = plan.get("sql", plan.get("code", ""))
        if sql_code:
            docs.append(RAGDocument(
                doc_id=f"declined:sql:{story_id}",
                doc_type=DocumentType.CONVENTION_RULE,
                content=f"ANTI-PATTERN (declined story {story_id}): Do NOT generate SQL like this.\n"
                        f"Reason: {reason}\nSQL: {sql_code[:500]}",
                metadata={"story_id": story_id, "origin": "declined_plan", "reason": reason},
            ))
        if docs:
            self._retriever.index_documents(docs)
            logger.info("Decline feedback: indexed %d anti-patterns from story %s", len(docs), story_id)
        return len(docs)

    def on_validator_corrections(self, story_id: str, corrections: list[dict]) -> int:
        """Index validator corrections as learning data."""
        docs = []
        for i, c in enumerate(corrections):
            issue = c.get("issue", c.get("finding", ""))
            fix = c.get("fix", c.get("correction", ""))
            if issue:
                docs.append(RAGDocument(
                    doc_id=f"correction:{story_id}:{i}",
                    doc_type=DocumentType.CONVENTION_RULE,
                    content=f"VALIDATION CORRECTION (story {story_id}):\n"
                            f"Issue: {issue}\nFix: {fix}",
                    metadata={"story_id": story_id, "origin": "validator_correction"},
                ))
        if docs:
            self._retriever.index_documents(docs)
            logger.info("Validator feedback: indexed %d corrections from story %s", len(docs), story_id)
        return len(docs)
