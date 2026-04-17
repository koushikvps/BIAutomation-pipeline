"""Tests for ConnectorClient security."""

import pytest
from shared.connector_client import ConnectorClient


class TestConnectorSecurity:
    def test_sanitize_identifier_removes_injection(self):
        result = ConnectorClient._sanitize_identifier("dbo]; DROP TABLE users--")
        assert "]" not in result
        assert "--" not in result
        assert "DROP" not in result or ";" not in result

    def test_sanitize_identifier_allows_valid(self):
        assert ConnectorClient._sanitize_identifier("dbo") == "dbo"
        assert ConnectorClient._sanitize_identifier("my_table") == "my_table"
        assert ConnectorClient._sanitize_identifier("schema.table") == "schema.table"

    def test_supported_types(self):
        client = ConnectorClient()
        assert "rest_api" in client.SUPPORTED_TYPES
        assert "csv_upload" in client.SUPPORTED_TYPES
        assert "azure_sql" in client.SUPPORTED_TYPES

    def test_extract_csv_limits_rows(self):
        client = ConnectorClient()
        csv_content = "name,value\n" + "\n".join([f"item{i},{i}" for i in range(100)])
        result = client._extract_csv({"content": csv_content}, limit=5)
        assert len(result["rows"]) == 5

    def test_extract_csv_empty(self):
        client = ConnectorClient()
        result = client._extract_csv({"content": ""}, limit=10)
        assert "error" in result
