"""Discovery-RAG Bridge: converts Discovery Agent + Convention Adapter output into RAG documents.

Integration Mode flow:
  1. Discovery Agent scans client environment → EnvironmentProfile
  2. Convention Adapter builds ruleset → ConventionRuleset
  3. THIS BRIDGE indexes both into the RAG knowledge base
  4. All subsequent agent LLM calls are grounded in discovered metadata

This is the key difference in Integration Mode: instead of the LLM guessing
what exists in the client's repo, it KNOWS because the Discovery scan
has been indexed into the vector store.
"""
from __future__ import annotations

import logging
from typing import Optional

from .rag_retriever import RAGRetriever, RAGDocument, DocumentType

logger = logging.getLogger(__name__)


class DiscoveryRAGBridge:
    """Indexes Discovery Agent output and Convention Adapter ruleset into RAG."""

    def __init__(self, retriever: Optional[RAGRetriever] = None):
        self._retriever = retriever or RAGRetriever()

    def index_environment_profile(self, profile: dict) -> dict:
        """Convert a full EnvironmentProfile into RAG documents and index them."""
        stats = {"tables": 0, "views": 0, "procs": 0, "conventions": 0,
                 "adf_pipelines": 0, "adls_containers": 0}
        docs = []

        synapse = profile.get("synapse", {})
        for schema_name, schema_data in synapse.get("schemas", {}).items():
            for t in schema_data.get("tables", []):
                cols = t.get("columns", [])
                col_lines = [f"  {c['name']} {c.get('type','')} {'NULL' if c.get('nullable')=='YES' else 'NOT NULL'}"
                             for c in cols]
                dist = t.get("distribution", "")
                dist_info = f" (distribution: {dist})" if dist and dist != "UNKNOWN" else ""
                docs.append(RAGDocument(
                    doc_id=f"disc:{schema_name}.{t['name']}",
                    doc_type=DocumentType.TABLE_SCHEMA,
                    content=f"DISCOVERED [{schema_name}].[{t['name']}]{dist_info}\nColumns:\n" + "\n".join(col_lines),
                    metadata={"schema": schema_name, "table": t["name"], "origin": "discovery",
                              "distribution": dist, "column_count": len(cols)},
                ))
                stats["tables"] += 1

                for c in cols:
                    docs.append(RAGDocument(
                        doc_id=f"disc:col:{schema_name}.{t['name']}.{c['name']}",
                        doc_type=DocumentType.COLUMN_DEF,
                        content=f"DISCOVERED [{schema_name}].[{t['name']}].{c['name']} ({c.get('type','')}, "
                                f"{'NULL' if c.get('nullable')=='YES' else 'NOT NULL'})",
                        metadata={"schema": schema_name, "table": t["name"], "column": c["name"],
                                  "origin": "discovery"},
                    ))

            for v in schema_data.get("views", []):
                cols = v.get("columns", [])
                col_lines = [f"  {c['name']} {c.get('type','')}" for c in cols]
                docs.append(RAGDocument(
                    doc_id=f"disc:view:{schema_name}.{v['name']}",
                    doc_type=DocumentType.TABLE_SCHEMA,
                    content=f"DISCOVERED VIEW [{schema_name}].[{v['name']}]\nColumns:\n" + "\n".join(col_lines),
                    metadata={"schema": schema_name, "view": v["name"], "origin": "discovery"},
                ))
                stats["views"] += 1

            for p in schema_data.get("procedures", []):
                docs.append(RAGDocument(
                    doc_id=f"disc:proc:{schema_name}.{p['name']}",
                    doc_type=DocumentType.SQL_PATTERN,
                    content=f"DISCOVERED PROC [{schema_name}].[{p['name']}]",
                    metadata={"schema": schema_name, "proc": p["name"], "origin": "discovery"},
                ))
                stats["procs"] += 1

        # ADF pipelines as SQL patterns (they show existing ETL logic)
        for pl in profile.get("adf", {}).get("pipelines", []):
            activities = pl.get("activities", [])
            act_text = ", ".join(f"{a['name']} ({a['type']})" for a in activities)
            docs.append(RAGDocument(
                doc_id=f"disc:adf:{pl['name']}",
                doc_type=DocumentType.SQL_PATTERN,
                content=f"DISCOVERED ADF Pipeline: {pl['name']}\nActivities: {act_text}",
                metadata={"pipeline": pl["name"], "origin": "discovery_adf"},
            ))
            stats["adf_pipelines"] += 1

        # ADLS container structure
        for cont in profile.get("adls", {}).get("containers", []):
            folders = cont.get("top_level_folders", [])
            formats = cont.get("file_formats", [])
            docs.append(RAGDocument(
                doc_id=f"disc:adls:{cont['name']}",
                doc_type=DocumentType.CONVENTION_RULE,
                content=f"DISCOVERED ADLS Container: {cont['name']}\n"
                        f"Folders: {', '.join(folders)}\nFile formats: {', '.join(formats)}",
                metadata={"container": cont["name"], "origin": "discovery_adls"},
            ))
            stats["adls_containers"] += 1

        if docs:
            self._retriever.index_documents(docs)

        stats["total_documents"] = self._retriever.document_count
        logger.info("Discovery→RAG bridge: indexed %d tables, %d views, %d procs, %d ADF pipelines",
                     stats["tables"], stats["views"], stats["procs"], stats["adf_pipelines"])
        return stats

    def index_convention_ruleset(self, ruleset) -> int:
        """Index a ConventionRuleset into RAG as convention rules."""
        docs = []
        if hasattr(ruleset, "to_prompt_string"):
            docs.append(RAGDocument(
                doc_id="disc:conventions:full",
                doc_type=DocumentType.CONVENTION_RULE,
                content=f"DISCOVERED CONVENTIONS (from client environment):\n{ruleset.to_prompt_string()}",
                metadata={"origin": "convention_adapter"},
            ))

        if hasattr(ruleset, "table_prefix") and ruleset.table_prefix:
            docs.append(RAGDocument(
                doc_id="disc:conventions:table_prefix",
                doc_type=DocumentType.CONVENTION_RULE,
                content=f"Convention: Table prefix is '{ruleset.table_prefix}_'",
                metadata={"origin": "convention_adapter", "type": "table_prefix"},
            ))
        if hasattr(ruleset, "view_prefix") and ruleset.view_prefix:
            docs.append(RAGDocument(
                doc_id="disc:conventions:view_prefix",
                doc_type=DocumentType.CONVENTION_RULE,
                content=f"Convention: View prefix is '{ruleset.view_prefix}_'",
                metadata={"origin": "convention_adapter", "type": "view_prefix"},
            ))
        if hasattr(ruleset, "schema_mapping") and ruleset.schema_mapping:
            for layer, schema in ruleset.schema_mapping.items():
                docs.append(RAGDocument(
                    doc_id=f"disc:conventions:schema:{layer}",
                    doc_type=DocumentType.CONVENTION_RULE,
                    content=f"Convention: {layer} layer uses schema [{schema}]",
                    metadata={"origin": "convention_adapter", "layer": layer, "schema": schema},
                ))
        if hasattr(ruleset, "naming_rules") and ruleset.naming_rules:
            for i, rule in enumerate(ruleset.naming_rules):
                docs.append(RAGDocument(
                    doc_id=f"disc:conventions:rule:{i}",
                    doc_type=DocumentType.CONVENTION_RULE,
                    content=f"Convention: {rule}",
                    metadata={"origin": "convention_adapter"},
                ))

        if docs:
            self._retriever.index_documents(docs)
            logger.info("Convention→RAG bridge: indexed %d convention rules", len(docs))
        return len(docs)

    def index_discovery_and_conventions(self, profile: dict, ruleset=None) -> dict:
        """One-call convenience: index both profile and ruleset."""
        stats = self.index_environment_profile(profile)
        if ruleset:
            stats["conventions"] = self.index_convention_ruleset(ruleset)
        return stats
