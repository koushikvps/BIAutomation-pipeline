"""Synapse SQL client using pyodbc with SQL auth for dedicated pool."""

from __future__ import annotations

import logging
import os
import re
import time
from contextlib import contextmanager
from typing import Generator

import pyodbc

from .config import AppConfig

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY_SEC = 2
TRANSIENT_ERRORS = {
    "08S01",  # Communication link failure
    "08001",  # Unable to connect
    "40613",  # Database not available
    "40197",  # Service error
    "40501",  # Service busy
    "49918",  # Not enough resources
    "49919",  # Cannot process request
    "49920",  # Too many operations
    "10928",  # Resource limit reached
    "10929",  # Resource limit reached
}


def _find_odbc_driver() -> str:
    for driver in [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
    ]:
        if driver in pyodbc.drivers():
            return driver
    available = pyodbc.drivers()
    raise RuntimeError(f"No SQL Server ODBC driver found. Available: {available}")


def _is_transient(error: pyodbc.Error) -> bool:
    sqlstate = getattr(error, "args", [None])[0] if error.args else None
    if sqlstate in TRANSIENT_ERRORS:
        return True
    msg = str(error).lower()
    return any(t in msg for t in ["timeout", "communication link", "connection was closed", "service busy"])


class SynapseClient:

    def __init__(self, config: AppConfig):
        self._endpoint = config.synapse_endpoint
        self._database = config.synapse_database
        self._driver = os.environ.get("ODBC_DRIVER") or _find_odbc_driver()
        self._user = os.environ.get("SYNAPSE_SQL_USER", "sqladmin")
        self._password = os.environ.get("SYNAPSE_SQL_PASSWORD", "")
        logger.info("SynapseClient: endpoint=%s, db=%s, driver=%s",
                     self._endpoint, self._database, self._driver)

    def _get_connection(self, database: str | None = None, autocommit: bool = False) -> pyodbc.Connection:
        db = database or self._database
        conn_str = (
            f"DRIVER={{{self._driver}}};"
            f"SERVER=tcp:{self._endpoint},1433;"
            f"DATABASE={db};"
            f"UID={self._user};"
            f"PWD={self._password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info("Connecting to %s/%s (attempt %d, autocommit=%s)",
                            self._endpoint, db, attempt, autocommit)
                conn = pyodbc.connect(conn_str, autocommit=autocommit)
                logger.info("Connected successfully")
                return conn
            except pyodbc.Error as e:
                if attempt < MAX_RETRIES and _is_transient(e):
                    delay = RETRY_DELAY_SEC * (2 ** (attempt - 1))
                    logger.warning("Transient connection error (attempt %d/%d), retrying in %ds: %s",
                                   attempt, MAX_RETRIES, delay, e)
                    time.sleep(delay)
                else:
                    raise

    @contextmanager
    def connection(self, database: str | None = None, autocommit: bool = False) -> Generator[pyodbc.Connection, None, None]:
        conn = self._get_connection(database, autocommit=autocommit)
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, sql: str, database: str | None = None, params: tuple = None) -> list[dict]:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                with self.connection(database) as conn:
                    cursor = conn.cursor()
                    if params:
                        cursor.execute(sql, params)
                    else:
                        cursor.execute(sql)
                    columns = [col[0] for col in cursor.description]
                    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    logger.info("Query returned %d rows", len(rows))
                    return rows
            except pyodbc.Error as e:
                if attempt < MAX_RETRIES and _is_transient(e):
                    delay = RETRY_DELAY_SEC * (2 ** (attempt - 1))
                    logger.warning("Transient query error (attempt %d/%d), retrying in %ds: %s",
                                   attempt, MAX_RETRIES, delay, e)
                    time.sleep(delay)
                else:
                    raise

    def execute_ddl(self, sql: str, database: str | None = None) -> None:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                with self.connection(database, autocommit=True) as conn:
                    cursor = conn.cursor()
                    for statement in self._split_go(sql):
                        statement = statement.strip()
                        if statement:
                            logger.info("Executing DDL: %.200s...", statement)
                            cursor.execute(statement)
                logger.info("DDL executed successfully")
                return
            except pyodbc.Error as e:
                if attempt < MAX_RETRIES and _is_transient(e):
                    delay = RETRY_DELAY_SEC * (2 ** (attempt - 1))
                    logger.warning("Transient DDL error (attempt %d/%d), retrying in %ds: %s",
                                   attempt, MAX_RETRIES, delay, e)
                    time.sleep(delay)
                else:
                    raise

    @staticmethod
    def _sanitize_identifier(value: str) -> str:
        """Remove any characters that aren't alphanumeric or underscore."""
        return re.sub(r"[^\w]", "", value)

    def check_object_exists(self, schema: str, name: str, database: str | None = None) -> bool:
        sql = """
        SELECT COUNT(*) AS cnt FROM (
            SELECT 1 AS found FROM sys.objects o
            JOIN sys.schemas sc ON o.schema_id = sc.schema_id
            WHERE sc.name = ? AND o.name = ?
            UNION ALL
            SELECT 1 AS found FROM sys.external_tables et
            JOIN sys.schemas sc ON et.schema_id = sc.schema_id
            WHERE sc.name = ? AND et.name = ?
        ) x
        """
        rows = self.execute_query(sql, database, params=(schema, name, schema, name))
        return rows and rows[0].get("cnt", 0) > 0

    def get_columns(self, schema: str, table: str, database: str | None = None) -> list[dict]:
        sql = """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        return self.execute_query(sql, database, params=(schema, table))

    @staticmethod
    def _split_go(sql: str) -> list[str]:
        return re.split(r"^\s*GO\s*$", sql, flags=re.MULTILINE | re.IGNORECASE)
