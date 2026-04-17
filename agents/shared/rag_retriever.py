"""RAG Retriever: grounds LLM calls in client-specific metadata to prevent hallucination.

Backend: Azure AI Search (managed service with hybrid vector + keyword search).

Storage architecture:
  - Azure AI Search index = durable, managed, survives restarts/scale-out
  - No local cache or ADLS persistence needed (Azure manages it)
  - Managed Identity auth (zero secrets)

Flow:
  Index:   push documents + embeddings to Azure AI Search index
  Read:    hybrid search (vector + BM25 keyword) → top-k results in milliseconds

If Azure AI Search is unavailable (local dev/demo), falls back to in-memory mode.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import numpy as np

from .config import AppConfig

logger = logging.getLogger(__name__)

EMBEDDING_DIM = 256
SEARCH_INDEX_NAME = os.environ.get("RAG_SEARCH_INDEX", "rag-knowledge-base")


class RAGBackend(str, Enum):
    AZURE_SEARCH = "azure_search"
    IN_MEMORY = "in_memory"


class DocumentType(str, Enum):
    TABLE_SCHEMA = "table_schema"
    COLUMN_DEF = "column_def"
    BUSINESS_TERM = "business_term"
    CONVENTION_RULE = "convention_rule"
    APPROVED_JOIN = "approved_join"
    SQL_PATTERN = "sql_pattern"
    LINEAGE = "lineage"


@dataclass
class RAGDocument:
    doc_id: str
    doc_type: DocumentType
    content: str
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"doc_id": self.doc_id, "doc_type": self.doc_type.value,
                "content": self.content, "metadata": self.metadata}

    @classmethod
    def from_dict(cls, d: dict) -> RAGDocument:
        return cls(doc_id=d["doc_id"], doc_type=DocumentType(d["doc_type"]),
                   content=d["content"], metadata=d.get("metadata", {}))


@dataclass
class RetrievalResult:
    documents: list[RAGDocument]
    query: str
    top_k: int

    def to_prompt_context(self) -> str:
        if not self.documents:
            return ""
        sections = {"table_schema": [], "column_def": [], "business_term": [],
                     "convention_rule": [], "approved_join": [], "sql_pattern": [], "lineage": []}
        for doc in self.documents:
            sections.get(doc.doc_type.value, []).append(doc.content)

        parts = []
        if sections["table_schema"]:
            parts.append("=== EXISTING TABLES (from client Synapse) ===\n" + "\n".join(sections["table_schema"]))
        if sections["column_def"]:
            parts.append("=== COLUMN DEFINITIONS ===\n" + "\n".join(sections["column_def"]))
        if sections["business_term"]:
            parts.append("=== BUSINESS GLOSSARY ===\n" + "\n".join(sections["business_term"]))
        if sections["convention_rule"]:
            parts.append("=== NAMING CONVENTIONS ===\n" + "\n".join(sections["convention_rule"]))
        if sections["approved_join"]:
            parts.append("=== APPROVED JOINS ===\n" + "\n".join(sections["approved_join"]))
        if sections["sql_pattern"]:
            parts.append("=== EXISTING SQL PATTERNS ===\n" + "\n".join(sections["sql_pattern"][:3]))
        if sections["lineage"]:
            parts.append("=== COLUMN LINEAGE ===\n" + "\n".join(sections["lineage"]))
        return "\n\n".join(parts)


def _text_to_embedding(text: str, dim: int = EMBEDDING_DIM) -> list[float]:
    """Deterministic text embedding using character-level hashing.

    For production, replace with Azure OpenAI text-embedding-ada-002.
    This hash-based approach is sufficient for keyword/schema matching.
    """
    tokens = re.findall(r'[a-zA-Z_][a-zA-Z0-9_]*', text.lower())
    vec = np.zeros(dim, dtype=np.float32)
    for token in tokens:
        h = int(hashlib.md5(token.encode()).hexdigest(), 16)
        for i in range(dim):
            bit = (h >> (i % 128)) & 1
            vec[i] += (1.0 if bit else -1.0)
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec /= norm
    return vec.tolist()


def _sanitize_key(doc_id: str) -> str:
    """Sanitize a document ID for use as an Azure AI Search key field."""
    return re.sub(r'[^a-zA-Z0-9_\-=]', '_', doc_id)[:900]


# -- Azure AI Search Index -------------------------------------------

class AzureSearchIndex:
    """Azure AI Search index for RAG retrieval with hybrid (vector + keyword) search."""

    def __init__(self, endpoint: str, index_name: str = SEARCH_INDEX_NAME):
        self._endpoint = endpoint
        self._index_name = index_name
        self._client = None
        self._available = False
        self._doc_count = 0
        self._init_client()

    def _init_client(self):
        try:
            from azure.identity import DefaultAzureCredential
            from azure.search.documents import SearchClient
            from azure.search.documents.indexes import SearchIndexClient
            from azure.search.documents.indexes.models import (
                SearchIndex,
                SearchField,
                SearchFieldDataType,
                SimpleField,
                SearchableField,
                VectorSearch,
                HnswAlgorithmConfiguration,
                VectorSearchProfile,
                SearchIndex,
            )

            cred = DefaultAzureCredential()

            idx_client = SearchIndexClient(endpoint=self._endpoint, credential=cred)
            try:
                idx_client.get_index(self._index_name)
                logger.info("Azure AI Search index '%s' already exists", self._index_name)
            except Exception:
                index_def = SearchIndex(
                    name=self._index_name,
                    fields=[
                        SimpleField(name="id", type=SearchFieldDataType.String, key=True, filterable=True),
                        SimpleField(name="doc_type", type=SearchFieldDataType.String, filterable=True),
                        SearchableField(name="content", type=SearchFieldDataType.String),
                        SimpleField(name="metadata_json", type=SearchFieldDataType.String),
                        SearchField(
                            name="content_vector",
                            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                            searchable=True,
                            vector_search_dimensions=EMBEDDING_DIM,
                            vector_search_profile_name="rag-vector-profile",
                        ),
                    ],
                    vector_search=VectorSearch(
                        algorithms=[HnswAlgorithmConfiguration(name="rag-hnsw")],
                        profiles=[VectorSearchProfile(name="rag-vector-profile", algorithm_configuration_name="rag-hnsw")],
                    ),
                )
                idx_client.create_index(index_def)
                logger.info("Created Azure AI Search index '%s'", self._index_name)

            self._client = SearchClient(
                endpoint=self._endpoint, index_name=self._index_name, credential=cred)
            self._available = True
            self._refresh_count()
            logger.info("Azure AI Search ready: %s (index: %s, docs: %d)",
                        self._endpoint, self._index_name, self._doc_count)
        except Exception as e:
            logger.warning("Azure AI Search unavailable (falling back to in-memory): %s", e)
            self._available = False

    def _refresh_count(self):
        try:
            result = self._client.search(search_text="*", top=0, include_total_count=True)
            self._doc_count = result.get_count() or 0
        except Exception:
            self._doc_count = 0

    @property
    def available(self) -> bool:
        return self._available

    @property
    def count(self) -> int:
        return self._doc_count

    def add_batch(self, docs: list[RAGDocument]):
        if not self._available or not docs:
            return
        from azure.search.documents.models import IndexingResult
        batch = []
        for doc in docs:
            batch.append({
                "id": _sanitize_key(doc.doc_id),
                "doc_type": doc.doc_type.value,
                "content": doc.content[:32000],
                "metadata_json": json.dumps(doc.metadata)[:8000],
                "content_vector": _text_to_embedding(doc.content),
            })
        try:
            chunk_size = 1000
            for i in range(0, len(batch), chunk_size):
                self._client.upload_documents(documents=batch[i:i + chunk_size])
            self._doc_count += len(batch)
            logger.info("Indexed %d documents into Azure AI Search", len(batch))
        except Exception as e:
            logger.error("Azure AI Search indexing failed: %s", e)

    def search(self, query: str, top_k: int = 10) -> list[RAGDocument]:
        if not self._available:
            return []
        try:
            from azure.search.documents.models import VectorizedQuery
            vector_query = VectorizedQuery(
                vector=_text_to_embedding(query),
                k_nearest_neighbors=top_k,
                fields="content_vector",
            )
            results = self._client.search(
                search_text=query,
                vector_queries=[vector_query],
                top=top_k,
                select=["id", "doc_type", "content", "metadata_json"],
            )
            docs = []
            for r in results:
                try:
                    meta = json.loads(r.get("metadata_json", "{}"))
                except (json.JSONDecodeError, TypeError):
                    meta = {}
                docs.append(RAGDocument(
                    doc_id=r["id"],
                    doc_type=DocumentType(r["doc_type"]),
                    content=r["content"],
                    metadata=meta,
                ))
            return docs
        except Exception as e:
            logger.error("Azure AI Search query failed: %s", e)
            return []

    def clear(self):
        if not self._available:
            return
        try:
            results = self._client.search(search_text="*", top=1000, select=["id"])
            ids = [{"id": r["id"]} for r in results]
            if ids:
                self._client.delete_documents(documents=ids)
            self._doc_count = 0
            logger.info("Cleared Azure AI Search index")
        except Exception as e:
            logger.error("Azure AI Search clear failed: %s", e)


# -- In-Memory Fallback (local dev / demo) ----------------------------

class InMemoryIndex:
    """Simple in-memory vector index for local dev when Azure AI Search is unavailable."""

    def __init__(self):
        self._documents: list[RAGDocument] = []
        self._embeddings: list[list[float]] = []
        self._doc_id_set: set[str] = set()

    def add_batch(self, docs: list[RAGDocument]):
        for doc in docs:
            if doc.doc_id in self._doc_id_set:
                continue
            self._embeddings.append(_text_to_embedding(doc.content))
            self._documents.append(doc)
            self._doc_id_set.add(doc.doc_id)

    def search(self, query: str, top_k: int = 10) -> list[RAGDocument]:
        if not self._documents:
            return []
        q_vec = np.array(_text_to_embedding(query), dtype=np.float32)
        scores = []
        for i, emb in enumerate(self._embeddings):
            score = float(np.dot(q_vec, np.array(emb, dtype=np.float32)))
            if score > 0.0:
                scores.append((score, i))
        scores.sort(reverse=True)
        return [self._documents[idx] for _, idx in scores[:top_k]]

    @property
    def count(self) -> int:
        return len(self._documents)

    def clear(self):
        self._documents.clear()
        self._embeddings.clear()
        self._doc_id_set.clear()


# -- Main Retriever ---------------------------------------------------

class RAGRetriever:
    """Main RAG interface used by all agents.

    Primary backend: Azure AI Search (managed, hybrid vector + keyword).
    Fallback: In-memory index for local dev / demo.
    """

    def __init__(self, config: Optional[AppConfig] = None, backend: RAGBackend = RAGBackend.AZURE_SEARCH):
        self._config = config
        self._backend = backend
        self._search_index: Optional[AzureSearchIndex] = None
        self._memory_index: Optional[InMemoryIndex] = None

        search_endpoint = (
            config.search_endpoint if config and hasattr(config, 'search_endpoint') and config.search_endpoint
            else os.environ.get("AZURE_SEARCH_ENDPOINT", "")
        )

        if backend == RAGBackend.AZURE_SEARCH and search_endpoint:
            self._search_index = AzureSearchIndex(search_endpoint)
            if self._search_index.available:
                logger.info("RAG using Azure AI Search: %d documents", self._search_index.count)
            else:
                logger.info("Azure AI Search unavailable, falling back to in-memory")
                self._backend = RAGBackend.IN_MEMORY
                self._memory_index = InMemoryIndex()
        else:
            logger.info("No Azure AI Search endpoint configured, using in-memory mode")
            self._backend = RAGBackend.IN_MEMORY
            self._memory_index = InMemoryIndex()

    def _active_index(self):
        if self._search_index and self._search_index.available:
            return self._search_index
        return self._memory_index

    def index_documents(self, documents: list[RAGDocument]):
        idx = self._active_index()
        if idx:
            idx.add_batch(documents)
            logger.info("RAG index updated: %d total documents", self.document_count)

    def retrieve(self, query: str, top_k: int = 15) -> RetrievalResult:
        idx = self._active_index()
        if idx:
            docs = idx.search(query, top_k)
            return RetrievalResult(documents=docs, query=query, top_k=top_k)
        return RetrievalResult(documents=[], query=query, top_k=top_k)

    def retrieve_as_context(self, query: str, top_k: int = 15) -> str:
        return self.retrieve(query, top_k).to_prompt_context()

    @property
    def document_count(self) -> int:
        idx = self._active_index()
        return idx.count if idx else 0

    def clear(self):
        idx = self._active_index()
        if idx:
            idx.clear()

    def get_status(self) -> dict:
        if self._search_index and self._search_index.available:
            backend_detail = f"Azure AI Search ({self._search_index._endpoint})"
        else:
            backend_detail = "in-memory (local dev)"
        return {
            "backend": self._backend.value,
            "document_count": self.document_count,
            "index_name": SEARCH_INDEX_NAME,
            "endpoint": backend_detail,
            "types": self._get_type_counts(),
        }

    def _get_type_counts(self) -> dict:
        if self._memory_index and self._backend == RAGBackend.IN_MEMORY:
            counts = {}
            for doc in self._memory_index._documents:
                counts[doc.doc_type.value] = counts.get(doc.doc_type.value, 0) + 1
            return counts
        return {}
