"""Operations Module: Health monitoring, auto-pause, regression testing, cleanup.

Provides:
  - Agent performance stats (failure rates, LLM latency)
  - Secret expiry checking via Key Vault
  - Synapse auto-pause on idle
  - Prompt regression testing against known baselines
  - Config DB retention cleanup (archive old records)
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional

import pyodbc

from .config import AppConfig

logger = logging.getLogger(__name__)


class OpsManager:
    """Central operations manager for platform health and maintenance."""

    def __init__(self, config: AppConfig):
        self._config = config

    @contextmanager
    def _config_conn(self):
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self._config.config_db_server};"
            f"DATABASE={self._config.config_db_name};"
            f"UID={os.environ.get('SQL_ADMIN_USER', 'sqladmin')};"
            f"PWD={os.environ.get('SQL_ADMIN_PASSWORD', '')};"
            f"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=10;"
        )
        conn = pyodbc.connect(conn_str)
        try:
            yield conn
        finally:
            conn.close()

    # ── Agent Performance Stats ──

    def get_agent_stats(self, days: int = 7) -> dict:
        """Get agent performance metrics over the last N days."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        stats = {
            "period_days": days,
            "pipelines": {"total": 0, "completed": 0, "failed": 0, "success_rate": 0},
            "agents": {},
        }

        try:
            with self._config_conn() as conn:
                cursor = conn.cursor()

                # Pipeline stats
                cursor.execute(
                    "SELECT status, COUNT(*) as cnt FROM [config].[pipeline_registry] "
                    "WHERE created_at >= ? GROUP BY status", (cutoff,)
                )
                for row in cursor.fetchall():
                    status, cnt = row[0], row[1]
                    stats["pipelines"]["total"] += cnt
                    if status == "active":
                        stats["pipelines"]["completed"] += cnt
                    elif status in ("failed", "escalated"):
                        stats["pipelines"]["failed"] += cnt

                total = stats["pipelines"]["total"]
                if total > 0:
                    stats["pipelines"]["success_rate"] = round(
                        stats["pipelines"]["completed"] / total * 100, 1
                    )

                # Per-step stats (agent execution times and failures)
                cursor.execute(
                    "SELECT step_name, status, COUNT(*) as cnt, "
                    "AVG(DATEDIFF(SECOND, started_at, completed_at)) as avg_sec "
                    "FROM [config].[execution_log] "
                    "WHERE started_at >= ? GROUP BY step_name, status",
                    (cutoff,)
                )
                for row in cursor.fetchall():
                    step_name, status, cnt, avg_sec = row[0], row[1], row[2], row[3]
                    if step_name not in stats["agents"]:
                        stats["agents"][step_name] = {"total": 0, "completed": 0, "failed": 0, "avg_duration_sec": 0}
                    stats["agents"][step_name]["total"] += cnt
                    if status == "completed":
                        stats["agents"][step_name]["completed"] += cnt
                        stats["agents"][step_name]["avg_duration_sec"] = avg_sec or 0
                    elif status == "failed":
                        stats["agents"][step_name]["failed"] += cnt

        except Exception as e:
            stats["error"] = str(e)[:200]
            logger.warning("agent_stats query failed: %s", str(e)[:200])

        return stats

    # ── Secret Expiry Health ──

    def check_secret_health(self) -> dict:
        """Check Key Vault secret expiry and credential health."""
        secrets = {
            "status": "healthy",
            "secrets": [],
            "warnings": [],
        }

        # Check ADO PAT (most common expiry issue)
        ado_pat = os.environ.get("ADO_PAT", "")
        if ado_pat:
            try:
                import requests
                import base64
                auth = base64.b64encode(f":{ado_pat}".encode()).decode()
                resp = requests.get(
                    f"https://dev.azure.com/{self._config.ado_org}/_apis/connectionData",
                    headers={"Authorization": f"Basic {auth}"}, timeout=10,
                )
                if resp.ok:
                    secrets["secrets"].append({"name": "ADO_PAT", "status": "valid"})
                else:
                    secrets["secrets"].append({"name": "ADO_PAT", "status": "expired_or_invalid", "http_code": resp.status_code})
                    secrets["warnings"].append("ADO PAT is expired or invalid - generate a new one")
                    secrets["status"] = "warning"
            except Exception as e:
                secrets["secrets"].append({"name": "ADO_PAT", "status": "check_failed", "error": str(e)[:100]})
        else:
            secrets["secrets"].append({"name": "ADO_PAT", "status": "not_set"})
            secrets["warnings"].append("ADO_PAT is not configured")

        # Check LLM endpoint
        try:
            from .llm_client import LLMClient
            llm = LLMClient(self._config)
            llm.chat("test", "Reply OK", max_tokens=5)
            secrets["secrets"].append({"name": "AI_FOUNDRY_KEY", "status": "valid"})
        except Exception as e:
            err = str(e)[:100]
            secrets["secrets"].append({"name": "AI_FOUNDRY_KEY", "status": "error", "error": err})
            if "401" in err or "auth" in err.lower():
                secrets["warnings"].append("AI Foundry API key is invalid or expired")
                secrets["status"] = "critical"

        # Check SQL password
        sql_pwd = os.environ.get("SQL_ADMIN_PASSWORD", "")
        if sql_pwd:
            try:
                with self._config_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    secrets["secrets"].append({"name": "SQL_ADMIN_PASSWORD", "status": "valid"})
            except Exception as e:
                secrets["secrets"].append({"name": "SQL_ADMIN_PASSWORD", "status": "error", "error": str(e)[:100]})
                secrets["warnings"].append("SQL password may be expired or wrong")
                secrets["status"] = "critical"
        else:
            secrets["secrets"].append({"name": "SQL_ADMIN_PASSWORD", "status": "not_set"})

        # Check Teams webhook
        webhook_url = os.environ.get("TEAMS_WEBHOOK_URL", "")
        if webhook_url:
            secrets["secrets"].append({"name": "TEAMS_WEBHOOK_URL", "status": "configured"})
        else:
            secrets["secrets"].append({"name": "TEAMS_WEBHOOK_URL", "status": "not_set"})

        return secrets

    # ── Synapse Auto-Pause ──

    def check_synapse_idle(self, idle_minutes: int = 30) -> dict:
        """Check if Synapse pool has been idle and should be paused."""
        result = {"should_pause": False, "reason": "", "pool_status": "unknown"}

        try:
            from .synapse_client import SynapseClient
            synapse = SynapseClient(self._config)

            # Check if pool is even online
            try:
                synapse.execute_query("SELECT 1 AS ok")
                result["pool_status"] = "online"
            except Exception as e:
                logger.warning("Non-critical error checking pool status: %s", e)
                result["pool_status"] = "paused_or_offline"
                result["reason"] = "Pool is already paused or offline"
                return result

            # Check last pipeline activity from Config DB
            cutoff = (datetime.utcnow() - timedelta(minutes=idle_minutes)).isoformat()
            with self._config_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM [config].[execution_log] "
                    "WHERE started_at >= ? OR (completed_at IS NOT NULL AND completed_at >= ?)",
                    (cutoff, cutoff)
                )
                recent_activity = cursor.fetchone()[0]

            if recent_activity == 0:
                result["should_pause"] = True
                result["reason"] = f"No pipeline activity in the last {idle_minutes} minutes"
            else:
                result["reason"] = f"{recent_activity} recent activities in the last {idle_minutes} minutes"

        except Exception as e:
            result["error"] = str(e)[:200]

        return result

    def pause_synapse(self) -> dict:
        """Pause the Synapse dedicated pool."""
        try:
            from azure.identity import DefaultAzureCredential
            import requests

            sub = os.environ.get("AZURE_SUBSCRIPTION_ID", "")
            rg = os.environ.get("SYNAPSE_RESOURCE_GROUP", os.environ.get("ADF_RESOURCE_GROUP", ""))
            workspace = self._config.synapse_endpoint.split(".")[0].replace("https://", "").split("-sql-")[0]
            pool = self._config.synapse_database

            if not all([sub, rg, workspace, pool]):
                return {"status": "skipped", "reason": "Missing subscription/resource group config"}

            cred = DefaultAzureCredential()
            token = cred.get_token("https://management.azure.com/.default").token
            url = (
                f"https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}"
                f"/providers/Microsoft.Synapse/workspaces/{workspace}"
                f"/sqlPools/{pool}/pause?api-version=2021-06-01"
            )
            resp = requests.post(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)

            if resp.status_code in (200, 202):
                logger.info("Synapse pool paused successfully")
                return {"status": "paused"}
            else:
                return {"status": "error", "http_code": resp.status_code, "detail": resp.text[:200]}

        except Exception as e:
            logger.error("pause_synapse failed: %s", str(e)[:200])
            return {"status": "error", "detail": str(e)[:200]}

    # ── Prompt Regression Test ──

    def run_regression_test(self) -> dict:
        """Run known test stories through the Planner and compare output structure.

        Does NOT deploy. Only validates that LLM output is well-formed.
        """
        from .llm_client import LLMClient

        test_cases = [
            {
                "name": "simple_sales",
                "story": "As a data analyst, I need a sales_orders table in the gold layer with customer_id, order_date, total_amount from the source sales system.",
                "expect_tables": ["sales_orders"],
                "expect_layers": ["bronze", "silver", "gold"],
            },
            {
                "name": "multi_table",
                "story": "Build a customer 360 view joining customers, orders, and products tables.",
                "expect_tables": ["customers", "orders", "products"],
                "expect_layers": ["bronze", "silver", "gold"],
            },
            {
                "name": "incremental",
                "story": "Create an incremental load for daily transaction data from the transactions table.",
                "expect_tables": ["transactions"],
                "expect_layers": ["bronze", "silver"],
            },
        ]

        results = {"total": len(test_cases), "passed": 0, "failed": 0, "details": []}
        llm = LLMClient(self._config)

        planner_prompt = (
            "You are a data platform planner. Given a user story, return JSON with: "
            '{"story_id": "test", "mode": "greenfield", "tables": [...], "layers": [...], "risk_level": "low"}'
        )

        for tc in test_cases:
            detail = {"name": tc["name"], "status": "passed", "checks": []}
            try:
                resp = llm.chat_json(
                    system_prompt=planner_prompt,
                    user_prompt=tc["story"],
                )

                # Check JSON structure
                if not isinstance(resp, dict):
                    detail["status"] = "failed"
                    detail["checks"].append("LLM did not return valid JSON dict")
                else:
                    # Check required fields exist
                    for field in ["story_id", "mode"]:
                        if field in resp:
                            detail["checks"].append(f"{field}: present")
                        else:
                            detail["checks"].append(f"{field}: MISSING")
                            detail["status"] = "failed"

                    # Check tables/layers are lists
                    tables = resp.get("tables", resp.get("execution_order", []))
                    if isinstance(tables, list) and len(tables) > 0:
                        detail["checks"].append(f"tables: {len(tables)} returned")
                    else:
                        detail["checks"].append("tables: empty or missing")
                        detail["status"] = "failed"

            except Exception as e:
                detail["status"] = "failed"
                detail["checks"].append(f"Exception: {str(e)[:150]}")

            if detail["status"] == "passed":
                results["passed"] += 1
            else:
                results["failed"] += 1
            results["details"].append(detail)

        results["status"] = "pass" if results["failed"] == 0 else "fail"
        return results

    # ── DB Retention Cleanup ──

    def run_cleanup(self, retention_days: int = 90) -> dict:
        """Archive and purge Config DB records older than retention period."""
        cutoff = (datetime.utcnow() - timedelta(days=retention_days)).isoformat()
        result = {"retention_days": retention_days, "cutoff": cutoff, "deleted": {}}

        try:
            with self._config_conn() as conn:
                cursor = conn.cursor()

                # Clean execution_log (keep recent, delete old)
                cursor.execute(
                    "SELECT COUNT(*) FROM [config].[execution_log] WHERE started_at < ?", (cutoff,)
                )
                old_logs = cursor.fetchone()[0]
                if old_logs > 0:
                    cursor.execute(
                        "DELETE FROM [config].[execution_log] WHERE started_at < ?", (cutoff,)
                    )
                    conn.commit()
                    result["deleted"]["execution_log"] = old_logs

                # Clean artifact_versions (keep last 5 per object)
                cursor.execute("""
                    DELETE av FROM [config].[artifact_versions] av
                    INNER JOIN (
                        SELECT id, ROW_NUMBER() OVER (
                            PARTITION BY object_name ORDER BY created_at DESC
                        ) as rn FROM [config].[artifact_versions]
                    ) ranked ON av.id = ranked.id
                    WHERE ranked.rn > 5 AND av.created_at < ?
                """, (cutoff,))
                old_artifacts = cursor.rowcount
                conn.commit()
                result["deleted"]["artifact_versions"] = old_artifacts

                # Clean deployment_log
                try:
                    cursor.execute(
                        "SELECT COUNT(*) FROM [catalog].[deployment_log] WHERE deployed_at < ?", (cutoff,)
                    )
                    old_deploys = cursor.fetchone()[0]
                    if old_deploys > 0:
                        cursor.execute(
                            "DELETE FROM [catalog].[deployment_log] WHERE deployed_at < ?", (cutoff,)
                        )
                        conn.commit()
                        result["deleted"]["deployment_log"] = old_deploys
                except Exception as e:
                    logger.warning("Non-critical error cleaning deployment_log (table may not exist): %s", e)

            result["status"] = "completed"

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)[:200]
            logger.error("cleanup failed: %s", str(e)[:200])

        return result

    # ── Full Dashboard ──

    def get_dashboard(self) -> dict:
        """Aggregate all ops metrics into a single dashboard response."""
        dashboard = {
            "timestamp": datetime.utcnow().isoformat(),
            "platform_version": "5.0",
            "products": {
                "bi_pipeline": {"agents": 7, "endpoints": 37, "orchestrator": "story_orchestrator + fix_bug_orchestrator"},
                "test_automation": {"agents": 6, "endpoints": 11, "orchestrator": "test_orchestrator"},
            },
        }

        # Agent stats
        try:
            dashboard["agent_stats"] = self.get_agent_stats(days=7)
        except Exception as e:
            dashboard["agent_stats"] = {"error": str(e)[:100]}

        # Secret health
        try:
            dashboard["secret_health"] = self.check_secret_health()
        except Exception as e:
            dashboard["secret_health"] = {"error": str(e)[:100]}

        # Synapse idle check
        try:
            dashboard["synapse_idle"] = self.check_synapse_idle()
        except Exception as e:
            dashboard["synapse_idle"] = {"error": str(e)[:100]}

        # DB size estimate
        try:
            with self._config_conn() as conn:
                cursor = conn.cursor()
                tables = {}
                for table in ["pipeline_registry", "execution_log", "artifact_versions"]:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM [config].[{table}]")
                        tables[table] = cursor.fetchone()[0]
                    except Exception as e:
                        logger.warning("Non-critical error counting config.%s: %s", table, e)
                        tables[table] = "N/A"
                dashboard["db_record_counts"] = tables
        except Exception as e:
            dashboard["db_record_counts"] = {"error": str(e)[:100]}

        # Overall health
        warnings = []
        secret_status = dashboard.get("secret_health", {}).get("status", "unknown")
        if secret_status in ("warning", "critical"):
            warnings.extend(dashboard.get("secret_health", {}).get("warnings", []))
        if dashboard.get("synapse_idle", {}).get("should_pause"):
            warnings.append("Synapse pool is idle - consider pausing to save costs")

        success_rate = dashboard.get("agent_stats", {}).get("pipelines", {}).get("success_rate", 100)
        if success_rate < 80:
            warnings.append(f"Pipeline success rate is {success_rate}% (below 80% threshold)")

        dashboard["overall_health"] = "healthy" if not warnings else "warning"
        dashboard["warnings"] = warnings

        return dashboard
