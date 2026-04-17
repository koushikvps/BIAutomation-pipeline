"""Source Catalog Scanner: reads source system schemas for greenfield deployments.

When there's no existing Synapse metadata, the client still has source systems
(SQL Server, Oracle, PostgreSQL, MySQL, CSV/Parquet on ADLS, API schemas).
This scanner reads the source and indexes it into the RAG knowledge base so the
LLM can generate the target medallion structure based on REAL source columns.

Supported sources:
  - SQL Server / Azure SQL (via ODBC)
  - PostgreSQL (via ODBC)
  - ADLS file sampling (Parquet headers, CSV headers)
  - Manual JSON upload (for any source)
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from .config import AppConfig
from .rag_retriever import RAGRetriever, RAGDocument, DocumentType

logger = logging.getLogger(__name__)


class SourceCatalogScanner:
    """Scans source systems and indexes their schema into the RAG knowledge base."""

    def __init__(self, config: AppConfig, retriever: Optional[RAGRetriever] = None):
        self._config = config
        self._retriever = retriever or RAGRetriever(config)

    def scan_source_db(self, connection_string: str, source_name: str = "source",
                       schemas: list[str] | None = None) -> dict:
        """Scan a SQL source database via ODBC and index all tables/columns."""
        import pyodbc
        stats = {"tables": 0, "columns": 0, "source": source_name, "errors": []}
        docs = []

        try:
            conn = pyodbc.connect(connection_string, timeout=30)
            cursor = conn.cursor()

            schema_filter = ""
            params = ()
            if schemas:
                placeholders = ",".join(["?" for _ in schemas])
                schema_filter = f" AND TABLE_SCHEMA IN ({placeholders})"
                params = tuple(schemas)

            cursor.execute(
                "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE "
                f"FROM INFORMATION_SCHEMA.TABLES WHERE 1=1{schema_filter} "
                "ORDER BY TABLE_SCHEMA, TABLE_NAME",
                params,
            )
            tables = [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]

            for t in tables:
                schema = t["TABLE_SCHEMA"]
                name = t["TABLE_NAME"]
                ttype = t.get("TABLE_TYPE", "BASE TABLE")

                cursor.execute(
                    "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
                    "CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE "
                    "FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                    (schema, name),
                )
                columns = [dict(zip([d[0] for d in cursor.description], row)) for row in cursor.fetchall()]

                col_lines = []
                for c in columns:
                    type_str = c["DATA_TYPE"]
                    if c.get("CHARACTER_MAXIMUM_LENGTH"):
                        type_str += f"({c['CHARACTER_MAXIMUM_LENGTH']})"
                    elif c.get("NUMERIC_PRECISION") and c.get("NUMERIC_SCALE"):
                        type_str += f"({c['NUMERIC_PRECISION']},{c['NUMERIC_SCALE']})"
                    nullable = "NULL" if c.get("IS_NULLABLE") == "YES" else "NOT NULL"
                    col_lines.append(f"  {c['COLUMN_NAME']} {type_str} {nullable}")

                    docs.append(RAGDocument(
                        doc_id=f"src:{source_name}:{schema}.{name}.{c['COLUMN_NAME']}",
                        doc_type=DocumentType.COLUMN_DEF,
                        content=f"SOURCE [{source_name}].[{schema}].[{name}].{c['COLUMN_NAME']} ({type_str}, {nullable})",
                        metadata={"source": source_name, "schema": schema, "table": name,
                                  "column": c["COLUMN_NAME"], "data_type": c["DATA_TYPE"],
                                  "origin": "source_system"},
                    ))
                    stats["columns"] += 1

                col_text = "\n".join(col_lines) if col_lines else "  (no columns)"
                docs.append(RAGDocument(
                    doc_id=f"src:{source_name}:{schema}.{name}",
                    doc_type=DocumentType.TABLE_SCHEMA,
                    content=f"SOURCE [{source_name}].[{schema}].[{name}] ({ttype})\nColumns:\n{col_text}",
                    metadata={"source": source_name, "schema": schema, "table": name,
                              "type": ttype, "column_count": len(columns), "origin": "source_system"},
                ))
                stats["tables"] += 1

            conn.close()
        except Exception as e:
            logger.error("Source DB scan failed for %s: %s", source_name, e)
            stats["errors"].append(str(e))

        if docs:
            self._retriever.index_documents(docs)
        stats["total_indexed"] = self._retriever.document_count
        return stats

    def scan_adls_files(self, container: str, path_prefix: str = "",
                        source_name: str = "adls") -> dict:
        """Sample file headers from ADLS (Parquet/CSV) and index column schemas."""
        stats = {"files": 0, "columns": 0, "source": source_name, "errors": []}
        docs = []

        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.filedatalake import DataLakeServiceClient

            account = self._config.storage_account_name
            cred = DefaultAzureCredential()
            service = DataLakeServiceClient(
                account_url=f"https://{account}.dfs.core.windows.net", credential=cred)
            fs = service.get_file_system_client(container)

            for path_item in fs.get_paths(path=path_prefix, recursive=True, max_results=100):
                if path_item.is_directory:
                    continue
                name = path_item.name
                ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""

                if ext == "parquet":
                    columns = self._read_parquet_schema(fs, name)
                elif ext == "csv":
                    columns = self._read_csv_headers(fs, name)
                else:
                    continue

                if columns:
                    col_lines = [f"  {c['name']} {c.get('type','STRING')}" for c in columns]
                    docs.append(RAGDocument(
                        doc_id=f"src:{source_name}:file:{name}",
                        doc_type=DocumentType.TABLE_SCHEMA,
                        content=f"SOURCE FILE [{source_name}]:/{name} ({ext})\nColumns:\n" + "\n".join(col_lines),
                        metadata={"source": source_name, "file": name, "format": ext,
                                  "column_count": len(columns), "origin": "source_file"},
                    ))
                    for c in columns:
                        docs.append(RAGDocument(
                            doc_id=f"src:{source_name}:file:{name}.{c['name']}",
                            doc_type=DocumentType.COLUMN_DEF,
                            content=f"SOURCE FILE [{source_name}]:/{name}.{c['name']} ({c.get('type','STRING')})",
                            metadata={"source": source_name, "file": name, "column": c["name"],
                                      "origin": "source_file"},
                        ))
                        stats["columns"] += 1
                    stats["files"] += 1

        except Exception as e:
            logger.error("ADLS file scan failed: %s", e)
            stats["errors"].append(str(e))

        if docs:
            self._retriever.index_documents(docs)
        stats["total_indexed"] = self._retriever.document_count
        return stats

    def ingest_manual_source_schema(self, tables: list[dict], source_name: str = "manual") -> dict:
        """Ingest a manually provided source schema (JSON array of tables with columns)."""
        stats = {"tables": 0, "columns": 0, "source": source_name}
        docs = []

        for t in tables:
            tbl_name = t.get("table", t.get("name", ""))
            schema = t.get("schema", "dbo")
            columns = t.get("columns", [])

            col_lines = []
            for c in columns:
                col_name = c.get("column", c.get("name", ""))
                col_type = c.get("type", c.get("data_type", "STRING"))
                nullable = c.get("nullable", "YES")
                desc = c.get("description", "")
                null_str = "NULL" if nullable == "YES" else "NOT NULL"
                col_lines.append(f"  {col_name} {col_type} {null_str}")

                content = f"SOURCE [{source_name}].[{schema}].[{tbl_name}].{col_name} ({col_type}, {null_str})"
                if desc:
                    content += f" -- {desc}"
                docs.append(RAGDocument(
                    doc_id=f"src:{source_name}:{schema}.{tbl_name}.{col_name}",
                    doc_type=DocumentType.COLUMN_DEF,
                    content=content,
                    metadata={"source": source_name, "schema": schema, "table": tbl_name,
                              "column": col_name, "origin": "source_manual"},
                ))
                stats["columns"] += 1

            col_text = "\n".join(col_lines) if col_lines else "  (no columns)"
            docs.append(RAGDocument(
                doc_id=f"src:{source_name}:{schema}.{tbl_name}",
                doc_type=DocumentType.TABLE_SCHEMA,
                content=f"SOURCE [{source_name}].[{schema}].[{tbl_name}]\nColumns:\n{col_text}",
                metadata={"source": source_name, "schema": schema, "table": tbl_name,
                           "column_count": len(columns), "origin": "source_manual"},
            ))
            stats["tables"] += 1

        if docs:
            self._retriever.index_documents(docs)
        stats["total_indexed"] = self._retriever.document_count
        return stats

    def _read_parquet_schema(self, fs_client, file_path: str) -> list[dict]:
        """Read column names/types from a Parquet file header."""
        try:
            file_client = fs_client.get_file_client(file_path)
            data = file_client.download_file(length=64 * 1024).readall()

            import tempfile
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp.write(data)
                tmp_path = tmp.name

            try:
                import pyarrow.parquet as pq
                schema = pq.read_schema(tmp_path)
                return [{"name": field.name, "type": str(field.type)} for field in schema]
            except ImportError:
                logger.warning("pyarrow not installed; skipping Parquet schema read")
                return []
            finally:
                os.unlink(tmp_path)
        except Exception as e:
            logger.warning("Failed to read Parquet schema from %s: %s", file_path, e)
            return []

    def _read_csv_headers(self, fs_client, file_path: str) -> list[dict]:
        """Read column names from a CSV file's header row."""
        try:
            file_client = fs_client.get_file_client(file_path)
            data = file_client.download_file(length=8 * 1024).readall()
            first_line = data.decode("utf-8", errors="replace").split("\n")[0].strip()
            if not first_line:
                return []
            headers = [h.strip().strip('"').strip("'") for h in first_line.split(",")]
            return [{"name": h, "type": "STRING"} for h in headers if h]
        except Exception as e:
            logger.warning("Failed to read CSV headers from %s: %s", file_path, e)
            return []
