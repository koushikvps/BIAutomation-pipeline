"""Tests for Ops Module."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from shared.ops import OpsManager


class TestOpsAgentStats:
    @pytest.fixture
    def ops(self, mock_config):
        return OpsManager(mock_config)

    def test_agent_stats_returns_structure(self, ops):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        # pipeline stats
        mock_cursor.fetchall.side_effect = [
            [("active", 10), ("failed", 2)],  # pipeline stats
            [("Planner", "completed", 8, 26), ("Planner", "failed", 2, None),
             ("Developer", "completed", 9, 15)],  # step stats
        ]
        with patch.object(ops, '_config_conn') as mock_cm:
            mock_cm.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_cm.return_value.__exit__ = MagicMock(return_value=False)
            stats = ops.get_agent_stats(days=7)

        assert stats["period_days"] == 7
        assert stats["pipelines"]["total"] == 12
        assert stats["pipelines"]["completed"] == 10
        assert stats["pipelines"]["failed"] == 2
        assert stats["pipelines"]["success_rate"] == 83.3

    def test_agent_stats_handles_db_error(self, ops):
        with patch.object(ops, '_config_conn', side_effect=Exception("DB down")):
            stats = ops.get_agent_stats()
        assert "error" in stats

    def test_agent_stats_zero_pipelines(self, ops):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.side_effect = [[], []]
        with patch.object(ops, '_config_conn') as mock_cm:
            mock_cm.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_cm.return_value.__exit__ = MagicMock(return_value=False)
            stats = ops.get_agent_stats()
        assert stats["pipelines"]["total"] == 0
        assert stats["pipelines"]["success_rate"] == 0


class TestOpsSecretHealth:
    @pytest.fixture
    def ops(self, mock_config):
        return OpsManager(mock_config)

    def test_secret_health_healthy(self, ops):
        mock_resp = MagicMock(ok=True)
        mock_llm = MagicMock()
        mock_llm.chat.return_value = "OK"
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = MagicMock()

        with patch("requests.get", return_value=mock_resp), \
             patch("shared.llm_client.LLMClient", return_value=mock_llm), \
             patch.object(ops, '_config_conn') as mock_cm:
            mock_cm.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_cm.return_value.__exit__ = MagicMock(return_value=False)
            health = ops.check_secret_health()
        assert health["status"] == "healthy"

    def test_secret_health_expired_pat(self, ops):
        mock_resp = MagicMock(ok=False, status_code=401)
        mock_llm = MagicMock()
        mock_llm.chat.return_value = "OK"
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = MagicMock()

        with patch("requests.get", return_value=mock_resp), \
             patch("shared.llm_client.LLMClient", return_value=mock_llm), \
             patch.object(ops, '_config_conn') as mock_cm:
            mock_cm.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_cm.return_value.__exit__ = MagicMock(return_value=False)
            health = ops.check_secret_health()
        assert health["status"] == "warning"
        assert any("PAT" in w for w in health["warnings"])


class TestOpsSynapseIdle:
    @pytest.fixture
    def ops(self, mock_config):
        return OpsManager(mock_config)

    def test_synapse_idle_when_no_activity(self, ops):
        mock_db_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0,)
        mock_db_conn.cursor.return_value = mock_cursor

        mock_syn = MagicMock()
        mock_syn.execute_query.return_value = [{"ok": 1}]

        with patch("shared.synapse_client.SynapseClient", return_value=mock_syn), \
             patch.dict("sys.modules", {"shared.synapse_client": MagicMock(SynapseClient=MagicMock(return_value=mock_syn))}), \
             patch.object(ops, '_config_conn') as mock_cm:
            mock_cm.return_value.__enter__ = MagicMock(return_value=mock_db_conn)
            mock_cm.return_value.__exit__ = MagicMock(return_value=False)
            result = ops.check_synapse_idle(idle_minutes=30)
        assert result["should_pause"] is True
        assert result["pool_status"] == "online"

    def test_synapse_already_paused(self, ops):
        mock_syn = MagicMock()
        mock_syn.execute_query.side_effect = Exception("paused")

        with patch.dict("sys.modules", {"shared.synapse_client": MagicMock(SynapseClient=MagicMock(return_value=mock_syn))}):
            result = ops.check_synapse_idle()
        assert result["pool_status"] == "paused_or_offline"
        assert result["should_pause"] is False

    def test_synapse_active(self, ops):
        mock_db_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (5,)
        mock_db_conn.cursor.return_value = mock_cursor

        mock_syn = MagicMock()
        mock_syn.execute_query.return_value = [{"ok": 1}]

        with patch("shared.synapse_client.SynapseClient", return_value=mock_syn), \
             patch.dict("sys.modules", {"shared.synapse_client": MagicMock(SynapseClient=MagicMock(return_value=mock_syn))}), \
             patch.object(ops, '_config_conn') as mock_cm:
            mock_cm.return_value.__enter__ = MagicMock(return_value=mock_db_conn)
            mock_cm.return_value.__exit__ = MagicMock(return_value=False)
            result = ops.check_synapse_idle()
        assert result["should_pause"] is False


class TestOpsCleanup:
    @pytest.fixture
    def ops(self, mock_config):
        return OpsManager(mock_config)

    def test_cleanup_deletes_old_records(self, ops):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (50,)
        mock_cursor.rowcount = 10
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(ops, '_config_conn') as mock_cm:
            mock_cm.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_cm.return_value.__exit__ = MagicMock(return_value=False)
            result = ops.run_cleanup(retention_days=90)

        assert result["status"] == "completed"
        assert result["retention_days"] == 90
        assert "execution_log" in result["deleted"]

    def test_cleanup_handles_error(self, ops):
        with patch.object(ops, '_config_conn', side_effect=Exception("DB down")):
            result = ops.run_cleanup()
        assert result["status"] == "error"


class TestOpsDashboard:
    @pytest.fixture
    def ops(self, mock_config):
        return OpsManager(mock_config)

    def test_dashboard_returns_structure(self, ops):
        with patch.object(ops, 'get_agent_stats', return_value={"pipelines": {"success_rate": 95}}), \
             patch.object(ops, 'check_secret_health', return_value={"status": "healthy", "warnings": []}), \
             patch.object(ops, 'check_synapse_idle', return_value={"should_pause": False}), \
             patch.object(ops, '_config_conn') as mock_cm:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (42,)
            mock_conn.cursor.return_value = mock_cursor
            mock_cm.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_cm.return_value.__exit__ = MagicMock(return_value=False)

            dashboard = ops.get_dashboard()

        assert "timestamp" in dashboard
        assert "platform_version" in dashboard
        assert "overall_health" in dashboard
        assert dashboard["overall_health"] == "healthy"

    def test_dashboard_warns_on_low_success_rate(self, ops):
        with patch.object(ops, 'get_agent_stats', return_value={"pipelines": {"success_rate": 60}}), \
             patch.object(ops, 'check_secret_health', return_value={"status": "healthy", "warnings": []}), \
             patch.object(ops, 'check_synapse_idle', return_value={"should_pause": True}), \
             patch.object(ops, '_config_conn') as mock_cm:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (0,)
            mock_conn.cursor.return_value = mock_cursor
            mock_cm.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_cm.return_value.__exit__ = MagicMock(return_value=False)

            dashboard = ops.get_dashboard()

        assert dashboard["overall_health"] == "warning"
        assert len(dashboard["warnings"]) >= 1
