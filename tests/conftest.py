"""Shared test fixtures for all tests."""

from __future__ import annotations

import os
import sys
import pytest
from unittest.mock import MagicMock, patch

# Add agents directory to path so imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

# Set required environment variables for tests
TEST_ENV = {
    "ENVIRONMENT": "test",
    "KEY_VAULT_URI": "https://test-kv.vault.azure.net/",
    "AZURE_OPENAI_ENDPOINT": "https://test-ai.openai.azure.com",
    "AZURE_OPENAI_DEPLOYMENT": "test-model",
    "SYNAPSE_SQL_ENDPOINT": "test-syn.sql.azuresynapse.net",
    "SYNAPSE_SQL_DATABASE": "testpool",
    "SYNAPSE_SQL_USER": "testuser",
    "SYNAPSE_SQL_PASSWORD": "TestPass123!",
    "SOURCE_DB_SERVER": "test-sql.database.windows.net",
    "SOURCE_DB_NAME": "test-sourcedb",
    "STORAGE_ACCOUNT_NAME": "teststorage",
    "AI_API_KEY": "test-api-key-not-real",
    "ADO_ORG": "TestOrg",
    "ADO_PROJECT": "TestProject",
    "ADO_PAT": "test-pat-not-real",
}


@pytest.fixture(autouse=True)
def set_test_env(monkeypatch):
    """Set test environment variables for all tests."""
    for key, value in TEST_ENV.items():
        monkeypatch.setenv(key, value)


@pytest.fixture
def mock_config():
    """Return a test AppConfig."""
    from shared.config import AppConfig
    return AppConfig(
        environment="test",
        key_vault_uri="https://test-kv.vault.azure.net/",
        openai_endpoint="https://test-ai.openai.azure.com",
        openai_deployment="test-model",
        synapse_endpoint="test-syn.sql.azuresynapse.net",
        synapse_database="testpool",
        source_db_server="test-sql.database.windows.net",
        source_db_name="test-sourcedb",
        storage_account_name="teststorage",
        ado_org="TestOrg",
        ado_project="TestProject",
        ado_repo="test-repo",
        config_db_server="test-sql.database.windows.net",
        config_db_name="test-sourcedb",
        search_endpoint="",
    )


@pytest.fixture
def mock_llm_client():
    """Return a mock LLM client that returns predictable responses."""
    mock = MagicMock()
    mock.chat.return_value = "OK"
    mock.chat_json.return_value = {"status": "ok"}
    mock.usage_stats = {"total_calls": 0, "total_prompt_tokens": 0, "total_completion_tokens": 0}
    return mock


@pytest.fixture
def sample_story():
    """Return a sample story input dict."""
    return {
        "story_id": "TEST-001",
        "work_item_id": 12345,
        "title": "Daily Sales Summary",
        "description": "As a business analyst, I want to see daily sales aggregated by region and product category.",
        "acceptance_criteria": [
            "Bronze layer: raw sales transactions",
            "Silver layer: cleaned and deduplicated",
            "Gold layer: aggregated by region and category",
        ],
        "source_tables": ["dbo.SalesTransactions", "dbo.Products", "dbo.Regions"],
    }


@pytest.fixture
def sample_build_plan():
    """Return a sample build plan dict."""
    return {
        "story_id": "TEST-001",
        "mode": {"value": "greenfield"},
        "risk_level": {"value": "low"},
        "execution_order": [
            {
                "step": 1,
                "layer": {"value": "bronze"},
                "action": "CREATE EXTERNAL TABLE",
                "artifact_type": {"value": "external_table"},
                "object_name": "bronze.SalesTransactions",
                "source": {"schema_name": "dbo", "table": "SalesTransactions"},
                "logic_summary": "External table over Parquet files",
                "load_pattern": "full",
            },
        ],
        "validation_requirements": [],
    }
