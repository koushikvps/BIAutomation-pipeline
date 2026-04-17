"""Column-level lineage tracker: records column transformations across layers."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Optional

import pyodbc

logger = logging.getLogger(__name__)


class LineageTracker:
    """Tracks column-level transformations from source → bronze → silver → gold."""

    def __init__(self, config=None):
        driver = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
        server = os.environ.get("CONFIG_DB_SERVER", os.environ.get("SOURCE_DB_SERVER", ""))
        database = os.environ.get("CONFIG_DB_NAME", os.environ.get("SOURCE_DB_NAME", ""))
        user = os.environ.get("SYNAPSE_SQL_USER", "sqladmin")
        password = os.environ.get("SYNAPSE_SQL_PASSWORD", "")
        self._conn_str = (
            f"DRIVER={{{driver}}};SERVER=tcp:{server},1433;DATABASE={database};"
            f"UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10;"
        ) if server and database else ""

    def _conn(self):
        return pyodbc.connect(self._conn_str, timeout=10, autocommit=True)

    def record_lineage(self, story_id: str, mappings: list[dict]) -> dict:
        """Store column lineage records.
        Each mapping: {source_schema, source_table, source_column,
                       target_schema, target_table, target_column,
                       transformation, layer_from, layer_to}
        """
        try:
            conn = self._conn()
            cur = conn.cursor()
            inserted = 0
            for m in mappings:
                cur.execute("""
                    INSERT INTO config.column_lineage
                    (story_id, source_schema, source_table, source_column,
                     target_schema, target_table, target_column,
                     transformation, layer_from, layer_to)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    story_id,
                    m.get("source_schema", ""),
                    m.get("source_table", ""),
                    m.get("source_column", ""),
                    m.get("target_schema", ""),
                    m.get("target_table", ""),
                    m.get("target_column", ""),
                    m.get("transformation", "direct"),
                    m.get("layer_from", "source"),
                    m.get("layer_to", "bronze"),
                )
                inserted += 1
            conn.close()
            return {"status": "ok", "records": inserted}
        except Exception as e:
            logger.warning("record_lineage failed: %s", e)
            return {"status": "error", "error": str(e)}

    def get_lineage(self, story_id: Optional[str] = None, target_table: Optional[str] = None) -> list[dict]:
        """Query lineage records."""
        try:
            conn = self._conn()
            cur = conn.cursor()
            query = "SELECT * FROM config.column_lineage WHERE 1=1"
            params = []
            if story_id:
                query += " AND story_id = ?"
                params.append(story_id)
            if target_table:
                query += " AND target_table = ?"
                params.append(target_table)
            query += " ORDER BY layer_from, source_table, source_column"
            cur.execute(query, *params)
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            conn.close()
            for r in rows:
                for k, v in r.items():
                    if hasattr(v, "isoformat"):
                        r[k] = v.isoformat()
            return rows
        except Exception as e:
            logger.warning("get_lineage failed: %s", e)
            return []

    def extract_lineage_from_sql(self, story_id: str, artifacts: list[dict]) -> list[dict]:
        """Parse generated SQL to extract column-level lineage automatically.
        Looks at CREATE EXTERNAL TABLE, CREATE VIEW, etc. to map columns.
        """
        mappings = []
        for art in artifacts:
            sql = art.get("content", "")
            layer = art.get("layer", "")
            obj_name = art.get("object_name", "")

            # Parse schema.name from object_name
            parts = obj_name.split(".")
            target_schema = parts[0] if len(parts) > 1 else layer
            target_table = parts[-1]

            # Extract column definitions
            col_pattern = re.compile(r'\[?(\w+)\]?\s+(?:NVARCHAR|VARCHAR|INT|BIGINT|DECIMAL|FLOAT|DATE|DATETIME|BIT)', re.IGNORECASE)
            for match in col_pattern.finditer(sql):
                col = match.group(1)
                if col.upper() in ("NULL", "NOT", "DEFAULT", "IDENTITY"):
                    continue
                mappings.append({
                    "source_schema": "sales" if layer == "bronze" else ("bronze" if layer == "silver" else "silver"),
                    "source_table": target_table.replace("ext_", "").replace("vw_", ""),
                    "source_column": col,
                    "target_schema": target_schema,
                    "target_table": target_table,
                    "target_column": col,
                    "transformation": "direct",
                    "layer_from": "source" if layer == "bronze" else ("bronze" if layer == "silver" else "silver"),
                    "layer_to": layer,
                })

            # Detect aggregations in gold views
            agg_pattern = re.compile(r'(SUM|COUNT|AVG|MIN|MAX)\s*\(\s*\[?(\w+)\]?\s*\)\s+(?:AS\s+)?\[?(\w+)\]?', re.IGNORECASE)
            for match in agg_pattern.finditer(sql):
                func_name, source_col, alias = match.groups()
                mappings.append({
                    "source_schema": "silver",
                    "source_table": target_table.replace("vw_", ""),
                    "source_column": source_col,
                    "target_schema": target_schema,
                    "target_table": target_table,
                    "target_column": alias,
                    "transformation": func_name.upper(),
                    "layer_from": "silver",
                    "layer_to": "gold",
                })

        return mappings

    def get_full_lineage_graph(self) -> dict:
        """Build a full lineage graph: source → bronze → silver → gold with columns."""
        rows = self.get_lineage()
        layers = {"source": {}, "bronze": {}, "silver": {}, "gold": {}}
        edges = []

        for r in rows:
            src_key = f"{r['source_schema']}.{r['source_table']}"
            tgt_key = f"{r['target_schema']}.{r['target_table']}"

            if r["layer_from"] not in layers:
                layers[r["layer_from"]] = {}
            if src_key not in layers[r["layer_from"]]:
                layers[r["layer_from"]][src_key] = []
            if r["source_column"] not in layers[r["layer_from"]][src_key]:
                layers[r["layer_from"]][src_key].append(r["source_column"])

            if r["layer_to"] not in layers:
                layers[r["layer_to"]] = {}
            if tgt_key not in layers[r["layer_to"]]:
                layers[r["layer_to"]][tgt_key] = []
            if r["target_column"] not in layers[r["layer_to"]][tgt_key]:
                layers[r["layer_to"]][tgt_key].append(r["target_column"])

            edges.append({
                "from": f"{r['layer_from']}.{src_key}.{r['source_column']}",
                "to": f"{r['layer_to']}.{tgt_key}.{r['target_column']}",
                "transformation": r.get("transformation", "direct"),
            })

        return {"layers": layers, "edges": edges}
