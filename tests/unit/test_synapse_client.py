"""Tests for SynapseClient."""

import pytest
from shared.synapse_client import SynapseClient


class TestSynapseClientSanitize:
    def test_sanitize_alphanumeric(self):
        assert SynapseClient._sanitize_identifier("gold") == "gold"

    def test_sanitize_underscore(self):
        assert SynapseClient._sanitize_identifier("my_table") == "my_table"

    def test_sanitize_removes_brackets(self):
        assert SynapseClient._sanitize_identifier("[gold]") == "gold"

    def test_sanitize_removes_quotes(self):
        assert SynapseClient._sanitize_identifier("'gold'; DROP TABLE--") == "goldDROPTABLE"

    def test_sanitize_removes_semicolons(self):
        assert SynapseClient._sanitize_identifier("table; DROP") == "tableDROP"

    def test_sanitize_removes_sql_injection(self):
        malicious = "test'] OR 1=1 --"
        result = SynapseClient._sanitize_identifier(malicious)
        assert "'" not in result
        assert "--" not in result
        assert "=" not in result


class TestSynapseClientSplitGo:
    def test_simple_split(self):
        sql = "SELECT 1\nGO\nSELECT 2"
        result = SynapseClient._split_go(sql)
        assert len(result) == 2

    def test_no_go(self):
        sql = "SELECT 1"
        result = SynapseClient._split_go(sql)
        assert len(result) == 1

    def test_case_insensitive(self):
        sql = "SELECT 1\ngo\nSELECT 2"
        result = SynapseClient._split_go(sql)
        assert len(result) == 2
