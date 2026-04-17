"""Catalog Indexer: syncs client Synapse metadata + glossary + conventions into RAG vector store.

Scans:
  1. Synapse INFORMATION_SCHEMA (tables, columns, types, distributions)
  2. Config DB business glossary
  3. Config DB approved joins
  4. Config DB naming conventions
  5. Convention ruleset (if Integration Mode was run)
  6. Column lineage records

Creates RAGDocument entries and indexes them into the RAG retriever.
"""
from __future__ import annotations

import logging
from typing import Optional

from .config import AppConfig
from .rag_retriever import RAGRetriever, RAGDocument, DocumentType

logger = logging.getLogger(__name__)


class CatalogIndexer:
    """Builds and refreshes the RAG knowledge base from client metadata."""

    def __init__(self, config: AppConfig, retriever: Optional[RAGRetriever] = None):
        self._config = config
        self._retriever = retriever or RAGRetriever(config)

    def full_sync(self) -> dict:
        """Run a complete sync of all metadata sources into the RAG index."""
        self._retriever.clear()
        stats = {"tables": 0, "columns": 0, "glossary": 0, "joins": 0,
                 "conventions": 0, "lineage": 0, "errors": []}

        stats["tables"], stats["columns"] = self._index_synapse_schema()
        stats["glossary"] = self._index_business_glossary()
        stats["joins"] = self._index_approved_joins()
        stats["conventions"] = self._index_naming_conventions()
        stats["lineage"] = self._index_column_lineage()

        stats["total"] = self._retriever.document_count
        logger.info("Catalog sync complete: %d documents indexed", stats["total"])
        return stats

    def _index_synapse_schema(self) -> tuple[int, int]:
        """Index table and column metadata from Synapse INFORMATION_SCHEMA."""
        docs = []
        table_count, col_count = 0, 0
        try:
            from .synapse_client import SynapseClient
            synapse = SynapseClient(self._config)

            tables = synapse.execute_query(
                "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE "
                "FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA, TABLE_NAME"
            )
            for t in tables:
                schema = t["TABLE_SCHEMA"]
                name = t["TABLE_NAME"]
                ttype = t.get("TABLE_TYPE", "BASE TABLE")
                doc_id = f"table:{schema}.{name}"
                columns = synapse.execute_query(
                    "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH "
                    "FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                    params=(schema, name)
                )
                col_lines = []
                for c in columns:
                    type_str = c["DATA_TYPE"]
                    if c.get("CHARACTER_MAXIMUM_LENGTH"):
                        type_str += f"({c['CHARACTER_MAXIMUM_LENGTH']})"
                    nullable = "NULL" if c.get("IS_NULLABLE") == "YES" else "NOT NULL"
                    col_lines.append(f"  {c['COLUMN_NAME']} {type_str} {nullable}")
                    col_doc = RAGDocument(
                        doc_id=f"col:{schema}.{name}.{c['COLUMN_NAME']}",
                        doc_type=DocumentType.COLUMN_DEF,
                        content=f"[{schema}].[{name}].{c['COLUMN_NAME']} ({type_str}, {nullable})",
                        metadata={"schema": schema, "table": name, "column": c["COLUMN_NAME"],
                                  "data_type": c["DATA_TYPE"]},
                    )
                    docs.append(col_doc)
                    col_count += 1

                col_text = "\n".join(col_lines) if col_lines else "  (no columns)"
                table_doc = RAGDocument(
                    doc_id=doc_id,
                    doc_type=DocumentType.TABLE_SCHEMA,
                    content=f"[{schema}].[{name}] ({ttype})\nColumns:\n{col_text}",
                    metadata={"schema": schema, "table": name, "type": ttype,
                              "column_count": len(columns)},
                )
                docs.append(table_doc)
                table_count += 1

        except Exception as e:
            logger.warning("Synapse schema indexing failed: %s", e)

        if docs:
            self._retriever.index_documents(docs)
        return table_count, col_count

    def _index_business_glossary(self) -> int:
        """Index business glossary terms from Config DB."""
        docs = []
        try:
            from .synapse_client import SynapseClient
            synapse = SynapseClient(self._config)
            rows = synapse.execute_query(
                "SELECT term, definition, category, synonyms "
                "FROM catalog.business_glossary"
            )
            for r in rows:
                term = r.get("term", "")
                definition = r.get("definition", "")
                category = r.get("category", "")
                synonyms = r.get("synonyms", "")
                content = f"Business Term: {term}\nDefinition: {definition}"
                if category:
                    content += f"\nCategory: {category}"
                if synonyms:
                    content += f"\nSynonyms: {synonyms}"
                docs.append(RAGDocument(
                    doc_id=f"glossary:{term}",
                    doc_type=DocumentType.BUSINESS_TERM,
                    content=content,
                    metadata={"term": term, "category": category},
                ))
        except Exception as e:
            logger.warning("Glossary indexing failed: %s", e)

        if docs:
            self._retriever.index_documents(docs)
        return len(docs)

    def _index_approved_joins(self) -> int:
        """Index approved join relationships from Config DB."""
        docs = []
        try:
            from .synapse_client import SynapseClient
            synapse = SynapseClient(self._config)
            rows = synapse.execute_query(
                "SELECT left_table, left_column, right_table, right_column, join_type, description "
                "FROM catalog.approved_joins"
            )
            for r in rows:
                content = (f"JOIN: [{r.get('left_table','')}].[{r.get('left_column','')}] "
                          f"{r.get('join_type','INNER')} JOIN "
                          f"[{r.get('right_table','')}].[{r.get('right_column','')}]")
                if r.get("description"):
                    content += f"\nDescription: {r['description']}"
                docs.append(RAGDocument(
                    doc_id=f"join:{r.get('left_table','')}.{r.get('left_column','')}-{r.get('right_table','')}.{r.get('right_column','')}",
                    doc_type=DocumentType.APPROVED_JOIN,
                    content=content,
                    metadata={"left_table": r.get("left_table", ""), "right_table": r.get("right_table", "")},
                ))
        except Exception as e:
            logger.warning("Approved joins indexing failed: %s", e)

        if docs:
            self._retriever.index_documents(docs)
        return len(docs)

    def _index_naming_conventions(self) -> int:
        """Index naming conventions from Config DB + convention rulesets."""
        docs = []
        try:
            from .synapse_client import SynapseClient
            synapse = SynapseClient(self._config)
            rows = synapse.execute_query(
                "SELECT convention_type, pattern, description, example "
                "FROM catalog.naming_conventions"
            )
            for r in rows:
                content = f"Convention: {r.get('convention_type','')}\nPattern: {r.get('pattern','')}"
                if r.get("description"):
                    content += f"\nDescription: {r['description']}"
                if r.get("example"):
                    content += f"\nExample: {r['example']}"
                docs.append(RAGDocument(
                    doc_id=f"convention:{r.get('convention_type','')}:{r.get('pattern','')}",
                    doc_type=DocumentType.CONVENTION_RULE,
                    content=content,
                    metadata={"type": r.get("convention_type", "")},
                ))
        except Exception as e:
            logger.warning("Convention indexing failed: %s", e)

        if docs:
            self._retriever.index_documents(docs)
        return len(docs)

    def _index_column_lineage(self) -> int:
        """Index column-level lineage from Config DB."""
        docs = []
        try:
            from .synapse_client import SynapseClient
            synapse = SynapseClient(self._config)
            rows = synapse.execute_query(
                "SELECT source_schema, source_table, source_column, "
                "target_schema, target_table, target_column, transformation "
                "FROM config.column_lineage"
            )
            for r in rows:
                src = f"[{r.get('source_schema','')}.{r.get('source_table','')}].{r.get('source_column','')}"
                tgt = f"[{r.get('target_schema','')}.{r.get('target_table','')}].{r.get('target_column','')}"
                transform = r.get("transformation", "direct")
                content = f"Lineage: {src} -> {tgt} ({transform})"
                docs.append(RAGDocument(
                    doc_id=f"lineage:{src}->{tgt}",
                    doc_type=DocumentType.LINEAGE,
                    content=content,
                    metadata={"source": src, "target": tgt},
                ))
        except Exception as e:
            logger.warning("Lineage indexing failed: %s", e)

        if docs:
            self._retriever.index_documents(docs)
        return len(docs)
