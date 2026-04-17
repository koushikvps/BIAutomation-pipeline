"""Multi-source connector framework: REST API, CSV, Azure SQL, Azure Blob, Snowflake, SAP."""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from typing import Optional

import pyodbc
import requests

logger = logging.getLogger(__name__)


class ConnectorClient:
    """Registry-driven connector that extracts data from any source."""

    SUPPORTED_TYPES = {"rest_api", "csv_upload", "azure_sql", "azure_blob", "snowflake", "sap"}

    def __init__(self, config=None):
        self._config = config
        driver = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
        server = os.environ.get("CONFIG_DB_SERVER", os.environ.get("SOURCE_DB_SERVER", ""))
        database = os.environ.get("CONFIG_DB_NAME", os.environ.get("SOURCE_DB_NAME", ""))
        user = os.environ.get("SYNAPSE_SQL_USER", "sqladmin")
        password = os.environ.get("SYNAPSE_SQL_PASSWORD", "")
        self._sql_conn_str = (
            f"DRIVER={{{driver}}};SERVER=tcp:{server},1433;DATABASE={database};"
            f"UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10;"
        ) if server and database else ""

    # ── registry helpers ────────────────────────────────────────
    def list_connectors(self) -> list[dict]:
        """Return all registered connectors from Config DB."""
        try:
            conn = pyodbc.connect(self._sql_conn_str, timeout=10)
            cur = conn.cursor()
            cur.execute("SELECT id, name, connector_type, status, last_tested_at, created_at FROM config.source_connectors ORDER BY name")
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            conn.close()
            for r in rows:
                for k, v in r.items():
                    if hasattr(v, "isoformat"):
                        r[k] = v.isoformat()
            return rows
        except Exception as e:
            logger.warning("list_connectors failed: %s", e)
            return []

    def register_connector(self, name: str, connector_type: str, connection_config: dict,
                           key_vault_secret: str = "", schema_hint: list[dict] = None) -> dict:
        """Register a new source connector."""
        if connector_type not in self.SUPPORTED_TYPES:
            return {"error": f"Unsupported type: {connector_type}. Supported: {self.SUPPORTED_TYPES}"}
        try:
            conn = pyodbc.connect(self._sql_conn_str, timeout=10)
            cur = conn.cursor()
            cur.execute("""
                IF EXISTS (SELECT 1 FROM config.source_connectors WHERE name = ?)
                    UPDATE config.source_connectors SET connector_type = ?, connection_config = ?, key_vault_secret = ?, schema_hint = ?, updated_at = GETUTCDATE() WHERE name = ?
                ELSE
                    INSERT INTO config.source_connectors (name, connector_type, connection_config, key_vault_secret, schema_hint) VALUES (?, ?, ?, ?, ?)
            """, name, connector_type, json.dumps(connection_config), key_vault_secret, json.dumps(schema_hint or []),
                name, name, connector_type, json.dumps(connection_config), key_vault_secret, json.dumps(schema_hint or []))
            conn.commit()
            conn.close()
            return {"status": "registered", "name": name}
        except Exception as e:
            return {"error": str(e)}

    def test_connector(self, connector_id: int) -> dict:
        """Test connectivity for a registered connector."""
        try:
            conn = pyodbc.connect(self._sql_conn_str, timeout=10)
            cur = conn.cursor()
            cur.execute("SELECT name, connector_type, connection_config, key_vault_secret FROM config.source_connectors WHERE id = ?", connector_id)
            row = cur.fetchone()
            if not row:
                conn.close()
                return {"error": "Connector not found"}
            name, ctype, config_json, kv_secret = row
            cfg = json.loads(config_json or "{}")

            result = self._test_connectivity(ctype, cfg)

            status = "active" if result.get("ok") else "error"
            cur.execute("UPDATE config.source_connectors SET status = ?, last_tested_at = GETUTCDATE() WHERE id = ?", status, connector_id)
            conn.commit()
            conn.close()
            return {"name": name, "type": ctype, **result}
        except Exception as e:
            return {"error": str(e)}

    def _test_connectivity(self, connector_type: str, config: dict) -> dict:
        """Actually test the connection."""
        try:
            if connector_type == "rest_api":
                url = config.get("base_url", "")
                resp = requests.get(url, timeout=10, headers=config.get("headers", {}))
                return {"ok": resp.status_code < 400, "status_code": resp.status_code}

            elif connector_type == "azure_sql":
                cs = config.get("connection_string", "")
                c = pyodbc.connect(cs, timeout=10)
                c.cursor().execute("SELECT 1")
                c.close()
                return {"ok": True}

            elif connector_type == "azure_blob":
                return {"ok": True, "note": "Blob connectivity validated via ADLS linked service"}

            elif connector_type == "csv_upload":
                return {"ok": True, "note": "CSV upload does not require connectivity test"}

            elif connector_type == "snowflake":
                return {"ok": False, "note": "Snowflake connector requires snowflake-connector-python"}

            elif connector_type == "sap":
                return {"ok": False, "note": "SAP connector requires pyrfc library"}

            return {"ok": False, "note": f"Unknown type: {connector_type}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ── data extraction ─────────────────────────────────────────
    def extract_preview(self, connector_type: str, config: dict, limit: int = 10) -> dict:
        """Extract a preview of data from a connector."""
        try:
            if connector_type == "rest_api":
                return self._extract_rest(config, limit)
            elif connector_type == "csv_upload":
                return self._extract_csv(config, limit)
            elif connector_type == "azure_sql":
                return self._extract_sql(config, limit)
            elif connector_type == "azure_blob":
                return self._extract_blob(config, limit)
            else:
                return {"error": f"Preview not yet supported for {connector_type}"}
        except Exception as e:
            return {"error": str(e)}

    def _extract_rest(self, config: dict, limit: int) -> dict:
        url = config.get("base_url", "")
        headers = config.get("headers", {})
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            rows = data[:limit]
        elif isinstance(data, dict):
            for key in ("value", "data", "results", "items", "records"):
                if key in data and isinstance(data[key], list):
                    rows = data[key][:limit]
                    break
            else:
                rows = [data]
        else:
            rows = []
        columns = list(rows[0].keys()) if rows else []
        return {"columns": columns, "rows": rows, "total_preview": len(rows)}

    def _extract_csv(self, config: dict, limit: int) -> dict:
        content = config.get("content", "")
        if not content:
            return {"error": "No CSV content provided"}
        reader = csv.DictReader(io.StringIO(content))
        rows = []
        for i, row in enumerate(reader):
            if i >= limit:
                break
            rows.append(dict(row))
        columns = list(rows[0].keys()) if rows else []
        return {"columns": columns, "rows": rows, "total_preview": len(rows)}

    @staticmethod
    def _sanitize_identifier(value: str) -> str:
        """Remove characters that aren't alphanumeric, underscore, or dot."""
        import re
        return re.sub(r"[^\w.]", "", value)

    def _extract_sql(self, config: dict, limit: int) -> dict:
        cs = config.get("connection_string", "")
        table = config.get("table", "")
        schema = config.get("schema", "dbo")
        if not cs or not table:
            return {"error": "connection_string and table required"}
        # Sanitize identifiers and validate limit
        schema = self._sanitize_identifier(schema)
        table = self._sanitize_identifier(table)
        limit = min(max(int(limit), 1), 1000)
        conn = pyodbc.connect(cs, timeout=10)
        cur = conn.cursor()
        cur.execute(f"SELECT TOP {limit} * FROM [{schema}].[{table}]")
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()
        for row in rows:
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    row[k] = v.isoformat()
        return {"columns": cols, "rows": rows, "total_preview": len(rows)}

    def _extract_blob(self, config: dict, limit: int) -> dict:
        return {"columns": [], "rows": [], "note": "Blob extraction via ADF Copy activity"}
