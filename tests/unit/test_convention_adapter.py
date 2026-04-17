"""Tests for Convention Adapter."""

import pytest

from shared.convention_adapter import (
    ConventionRuleset,
    build_ruleset_from_profile,
    apply_conventions_to_name,
)


class TestConventionRuleset:
    def test_defaults(self):
        r = ConventionRuleset()
        assert r.naming_case == "snake_case"
        assert r.bronze_schema == "bronze"
        assert r.view_prefix == "vw"
        assert r.delivery_mode == "direct"

    def test_to_dict(self):
        r = ConventionRuleset()
        d = r.to_dict()
        assert isinstance(d, dict)
        assert "naming_case" in d
        assert "bronze_schema" in d

    def test_to_prompt_context(self):
        r = ConventionRuleset()
        text = r.to_prompt_context()
        assert "CLIENT ENVIRONMENT CONVENTIONS" in text
        assert "snake_case" in text
        assert "bronze" in text


class TestBuildRulesetFromProfile:
    def test_empty_profile(self):
        r = build_ruleset_from_profile({})
        assert isinstance(r, ConventionRuleset)
        assert r.naming_case == "snake_case"

    def test_detects_snake_case(self):
        profile = {"conventions": {"naming_rules": ["All objects use snake_case naming"]}}
        r = build_ruleset_from_profile(profile)
        assert r.naming_case == "snake_case"

    def test_detects_pascal_case(self):
        profile = {"conventions": {"naming_rules": ["All objects use PascalCase naming"]}}
        r = build_ruleset_from_profile(profile)
        assert r.naming_case == "PascalCase"

    def test_maps_schema_to_bronze(self):
        profile = {"conventions": {
            "schema_patterns": {"raw": {"purpose": "external tables landing zone"}},
        }}
        r = build_ruleset_from_profile(profile)
        assert r.bronze_schema == "raw"
        assert r.bronze_object_type == "external_table"

    def test_maps_schema_to_silver(self):
        profile = {"conventions": {
            "schema_patterns": {"curated": {"purpose": "cleansed tables"}},
        }}
        r = build_ruleset_from_profile(profile)
        assert r.silver_schema == "curated"

    def test_maps_schema_to_gold(self):
        profile = {"conventions": {
            "schema_patterns": {"analytics": {"purpose": "view layer for reporting"}},
        }}
        r = build_ruleset_from_profile(profile)
        assert r.gold_schema == "analytics"
        assert r.gold_object_type == "view"

    def test_detects_table_prefix(self):
        profile = {"conventions": {"table_prefixes": {"tbl": 15}}}
        r = build_ruleset_from_profile(profile)
        assert r.table_prefix == "tbl"

    def test_detects_distribution(self):
        profile = {"conventions": {"common_distributions": {"HASH": 20, "ROUND_ROBIN": 5}}}
        r = build_ruleset_from_profile(profile)
        assert r.default_distribution == "HASH"

    def test_detects_adls_container(self):
        profile = {"adls": {"containers": [{"name": "landing"}, {"name": "archive"}]}}
        r = build_ruleset_from_profile(profile)
        assert r.adls_raw_container == "landing"

    def test_custom_rules_preserved(self):
        profile = {"conventions": {"naming_rules": ["Rule 1", "Rule 2"]}}
        r = build_ruleset_from_profile(profile)
        assert r.custom_rules == ["Rule 1", "Rule 2"]


class TestApplyConventions:
    def test_snake_case(self):
        r = ConventionRuleset()
        r.naming_case = "snake_case"
        r.view_prefix = "vw"
        result = apply_conventions_to_name("SalesReport", r, "view")
        assert result == "vw_sales_report"

    def test_pascal_case(self):
        r = ConventionRuleset()
        r.naming_case = "PascalCase"
        r.view_prefix = "vw"
        result = apply_conventions_to_name("sales_report", r, "view")
        assert result == "vw_SalesReport"

    def test_strips_existing_prefix(self):
        r = ConventionRuleset()
        r.naming_case = "snake_case"
        r.table_prefix = "tbl"
        result = apply_conventions_to_name("ext_Sales", r, "table")
        assert result == "tbl_sales"

    def test_procedure_prefix(self):
        r = ConventionRuleset()
        r.naming_case = "snake_case"
        r.proc_prefix = "usp"
        result = apply_conventions_to_name("LoadCustomers", r, "procedure")
        assert result == "usp_load_customers"

    def test_no_prefix_for_table_when_empty(self):
        r = ConventionRuleset()
        r.naming_case = "snake_case"
        r.table_prefix = ""
        result = apply_conventions_to_name("Sales", r, "table")
        assert result == "sales"
