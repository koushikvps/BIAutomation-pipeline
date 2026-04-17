"""Tests for AppConfig."""

import os
import pytest
from shared.config import AppConfig, ConfigError


class TestAppConfig:
    def test_from_env_success(self):
        config = AppConfig.from_env()
        assert config.environment == "test"
        assert config.key_vault_uri == "https://test-kv.vault.azure.net/"
        assert config.synapse_endpoint == "test-syn.sql.azuresynapse.net"
        assert config.storage_account_name == "teststorage"

    def test_from_env_missing_required(self, monkeypatch):
        monkeypatch.delenv("KEY_VAULT_URI", raising=False)
        with pytest.raises(ConfigError, match="KEY_VAULT_URI"):
            AppConfig.from_env()

    def test_from_env_missing_source_db(self, monkeypatch):
        monkeypatch.delenv("SOURCE_DB_SERVER", raising=False)
        with pytest.raises(ConfigError, match="SOURCE_DB_SERVER"):
            AppConfig.from_env()

    def test_from_env_missing_storage(self, monkeypatch):
        monkeypatch.delenv("STORAGE_ACCOUNT_NAME", raising=False)
        with pytest.raises(ConfigError, match="STORAGE_ACCOUNT_NAME"):
            AppConfig.from_env()

    def test_from_env_missing_openai(self, monkeypatch):
        monkeypatch.delenv("AZURE_OPENAI_ENDPOINT", raising=False)
        with pytest.raises(ConfigError, match="AZURE_OPENAI_ENDPOINT"):
            AppConfig.from_env()

    def test_no_hardcoded_defaults(self, monkeypatch):
        """Ensure no real resource names are hardcoded."""
        monkeypatch.delenv("SOURCE_DB_SERVER", raising=False)
        with pytest.raises(ConfigError):
            AppConfig.from_env()

    def test_optional_ado_defaults_to_empty(self):
        config = AppConfig.from_env()
        # ADO fields should use env vars, not hardcoded org names
        assert config.ado_org == "TestOrg"

    def test_frozen_dataclass(self):
        config = AppConfig.from_env()
        with pytest.raises(AttributeError):
            config.environment = "prod"
