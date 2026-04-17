"""State Registry: persists pipeline state to Azure SQL Config DB."""

from __future__ import annotations

import json
import logging
import os
import time
from contextlib import contextmanager
from typing import Generator, Optional

import pyodbc

from .config import AppConfig

logger = logging.getLogger(__name__)


class StateRegistry:

    def __init__(self, config: AppConfig):
        self._server = config.config_db_server
        self._database = config.config_db_name
        self._user = os.environ.get("SYNAPSE_SQL_USER", "sqladmin")
        self._password = os.environ.get("SYNAPSE_SQL_PASSWORD", "")
        driver = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
        self._conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER=tcp:{self._server},1433;"
            f"DATABASE={self._database};"
            f"UID={self._user};PWD={self._password};"
            f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )

    @contextmanager
    def _conn(self) -> Generator[pyodbc.Connection, None, None]:
        last_err = None
        for attempt in range(1, 4):
            try:
                conn = pyodbc.connect(self._conn_str, autocommit=True)
                try:
                    yield conn
                finally:
                    conn.close()
                return
            except pyodbc.Error as e:
                last_err = e
                msg = str(e).lower()
                if attempt < 3 and any(t in msg for t in ["timeout", "communication link", "service busy"]):
                    delay = 2 * (2 ** (attempt - 1))
                    logger.warning("StateRegistry connection retry %d/3 in %ds: %s", attempt, delay, e)
                    time.sleep(delay)
                else:
                    raise
        raise last_err

    def register_pipeline(
        self,
        story_id: str,
        work_item_id: Optional[int],
        title: str,
        source_tables: list[str],
        instance_id: str,
    ) -> int:
        with self._conn() as conn:
            cur = conn.cursor()
            # Upsert: if story already registered, update it
            cur.execute(
                "SELECT pipeline_id FROM config.pipeline_registry WHERE story_id = ?",
                story_id,
            )
            row = cur.fetchone()
            if row:
                pid = row[0]
                cur.execute("""
                    UPDATE config.pipeline_registry
                    SET work_item_id = ?, title = ?, source_tables = ?,
                        status = 'registered', last_instance_id = ?,
                        last_run_at = GETUTCDATE(), updated_at = GETUTCDATE()
                    WHERE pipeline_id = ?
                """, work_item_id, title, json.dumps(source_tables), instance_id, pid)
                logger.info("Updated pipeline_registry: pipeline_id=%d, story=%s", pid, story_id)
            else:
                cur.execute("""
                    INSERT INTO config.pipeline_registry
                    (story_id, work_item_id, title, source_tables, status, last_instance_id, last_run_at)
                    VALUES (?, ?, ?, ?, 'registered', ?, GETUTCDATE())
                """, story_id, work_item_id, title, json.dumps(source_tables), instance_id)
                cur.execute("SELECT SCOPE_IDENTITY()")
                pid = int(cur.fetchone()[0])
                logger.info("Registered pipeline: pipeline_id=%d, story=%s", pid, story_id)
            return pid

    def log_step(
        self,
        pipeline_id: int,
        instance_id: str,
        step_number: int,
        step_name: str,
        status: str,
        detail: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> int:
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO config.execution_log
                (pipeline_id, instance_id, step_number, step_name, status, detail, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, pipeline_id, instance_id, step_number, step_name, status, detail, error_message)
            cur.execute("SELECT SCOPE_IDENTITY()")
            log_id = int(cur.fetchone()[0])
            return log_id

    def complete_step(self, log_id: int, status: str = "completed", detail: Optional[str] = None):
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE config.execution_log
                SET status = ?, detail = COALESCE(?, detail),
                    finished_at = GETUTCDATE(),
                    duration_ms = DATEDIFF(MILLISECOND, started_at, GETUTCDATE())
                WHERE log_id = ?
            """, status, detail, log_id)

    def update_pipeline_status(
        self,
        pipeline_id: int,
        status: str,
        mode: Optional[str] = None,
        risk_level: Optional[str] = None,
        target_objects: Optional[list[str]] = None,
        artifact_count: Optional[int] = None,
        deploy_count: Optional[int] = None,
        skip_count: Optional[int] = None,
        fail_count: Optional[int] = None,
        duration_sec: Optional[int] = None,
    ):
        with self._conn() as conn:
            cur = conn.cursor()
            sets = ["status = ?", "updated_at = GETUTCDATE()"]
            params = [status]
            if mode:
                sets.append("mode = ?")
                params.append(mode)
            if risk_level:
                sets.append("risk_level = ?")
                params.append(risk_level)
            if target_objects is not None:
                sets.append("target_objects = ?")
                params.append(json.dumps(target_objects))
            if artifact_count is not None:
                sets.append("artifact_count = ?")
                params.append(artifact_count)
            if deploy_count is not None:
                sets.append("deploy_count = ?")
                params.append(deploy_count)
            if skip_count is not None:
                sets.append("skip_count = ?")
                params.append(skip_count)
            if fail_count is not None:
                sets.append("fail_count = ?")
                params.append(fail_count)
            if duration_sec is not None:
                sets.append("last_duration_sec = ?")
                params.append(duration_sec)
            params.append(pipeline_id)
            cur.execute(
                f"UPDATE config.pipeline_registry SET {', '.join(sets)} WHERE pipeline_id = ?",
                *params,
            )

    def save_artifact(
        self,
        pipeline_id: int,
        instance_id: str,
        layer: str,
        object_name: str,
        artifact_type: str,
        sql_content: str,
        file_path: Optional[str] = None,
        deploy_status: Optional[str] = None,
    ) -> int:
        with self._conn() as conn:
            cur = conn.cursor()
            # Get next version for this object
            cur.execute(
                "SELECT ISNULL(MAX(version), 0) + 1 FROM config.artifact_versions WHERE object_name = ? AND layer = ?",
                object_name, layer,
            )
            version = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO config.artifact_versions
                (pipeline_id, instance_id, layer, object_name, artifact_type,
                 file_path, sql_content, version, deploy_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, pipeline_id, instance_id, layer, object_name, artifact_type,
                file_path, sql_content, version, deploy_status)
            cur.execute("SELECT SCOPE_IDENTITY()")
            return int(cur.fetchone()[0])

    def update_artifact_commit(self, artifact_id: int, commit_sha: str, commit_branch: str):
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE config.artifact_versions
                SET commit_sha = ?, commit_branch = ?
                WHERE artifact_id = ?
            """, commit_sha, commit_branch, artifact_id)

    def get_pipeline_history(self, limit: int = 20) -> list[dict]:
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT TOP(?) pipeline_id, story_id, work_item_id, title, mode,
                       risk_level, status, artifact_count, deploy_count, skip_count,
                       fail_count, last_duration_sec, last_run_at, created_at
                FROM config.pipeline_registry
                ORDER BY last_run_at DESC
            """, limit)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_execution_steps(self, pipeline_id: int, instance_id: Optional[str] = None) -> list[dict]:
        with self._conn() as conn:
            cur = conn.cursor()
            if instance_id:
                cur.execute("""
                    SELECT log_id, step_number, step_name, status, detail,
                           error_message, started_at, finished_at, duration_ms
                    FROM config.execution_log
                    WHERE pipeline_id = ? AND instance_id = ?
                    ORDER BY step_number
                """, pipeline_id, instance_id)
            else:
                cur.execute("""
                    SELECT TOP(20) log_id, step_number, step_name, status, detail,
                           error_message, started_at, finished_at, duration_ms
                    FROM config.execution_log
                    WHERE pipeline_id = ?
                    ORDER BY log_id DESC
                """, pipeline_id)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_artifact_history(self, object_name: str) -> list[dict]:
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT artifact_id, pipeline_id, layer, object_name, artifact_type,
                       version, commit_sha, commit_branch, deploy_status, created_at
                FROM config.artifact_versions
                WHERE object_name = ?
                ORDER BY version DESC
            """, object_name)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
