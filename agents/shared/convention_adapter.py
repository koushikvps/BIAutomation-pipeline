"""Convention Adapter: Translates discovered naming patterns into rules for agents.

Takes the Discovery Agent's EnvironmentProfile and generates a ConventionRuleset
that all other agents (Planner, Developer, Code Review) consume as context.

This ensures generated SQL/ADF matches the client's existing patterns.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)


class ConventionRuleset:
    """Rules that govern how agents generate artifacts for a specific client environment."""

    def __init__(self):
        self.table_prefix: str = ""
        self.view_prefix: str = "vw"
        self.proc_prefix: str = "sp"
        self.naming_case: str = "snake_case"  # snake_case | PascalCase | camelCase
        self.bronze_schema: str = "bronze"
        self.silver_schema: str = "silver"
        self.gold_schema: str = "gold"
        self.bronze_object_type: str = "external_table"  # external_table | table
        self.silver_object_type: str = "table"  # table | view
        self.gold_object_type: str = "view"  # view | table
        self.default_distribution: str = "ROUND_ROBIN"
        self.use_columnstore_index: bool = True
        self.adf_pipeline_prefix: str = "pl"
        self.adf_dataset_prefix: str = "ds"
        self.adls_raw_container: str = "bronze"
        self.adls_path_pattern: str = "{schema}/{table}/{year}/{month}/{day}"
        self.delivery_mode: str = "direct"  # direct | pull_request
        self.pr_target_branch: str = "develop"
        self.pr_repo: str = ""
        self.custom_rules: list[str] = []

    def to_dict(self) -> dict:
        return self.__dict__.copy()

    def to_prompt_context(self) -> str:
        """Generate a text block that can be injected into LLM prompts."""
        lines = [
            "=== CLIENT ENVIRONMENT CONVENTIONS ===",
            f"Naming convention: {self.naming_case}",
            f"Bronze layer: schema [{self.bronze_schema}], object type: {self.bronze_object_type}",
            f"Silver layer: schema [{self.silver_schema}], object type: {self.silver_object_type}",
            f"Gold layer: schema [{self.gold_schema}], object type: {self.gold_object_type}",
            f"Table prefix: '{self.table_prefix}_' (if any)" if self.table_prefix else "Table prefix: none",
            f"View prefix: '{self.view_prefix}_'",
            f"Procedure prefix: '{self.proc_prefix}_'",
            f"Default distribution: {self.default_distribution}",
            f"Use Clustered Columnstore Index: {self.use_columnstore_index}",
            f"ADF pipeline naming: {self.adf_pipeline_prefix}_<source>_<target>_<frequency>",
            f"ADLS path pattern: {self.adls_path_pattern}",
            f"Delivery mode: {self.delivery_mode}",
        ]
        if self.custom_rules:
            lines.append("Custom rules:")
            for rule in self.custom_rules:
                lines.append(f"  - {rule}")
        lines.append("=== END CONVENTIONS ===")
        return "\n".join(lines)


def build_ruleset_from_profile(profile: dict) -> ConventionRuleset:
    """Convert a Discovery Agent's EnvironmentProfile into a ConventionRuleset."""
    ruleset = ConventionRuleset()
    conventions = profile.get("conventions", {})
    synapse = profile.get("synapse", {})
    adf = profile.get("adf", {})
    adls = profile.get("adls", {})

    # Naming case
    for rule in conventions.get("naming_rules", []):
        if "snake_case" in rule:
            ruleset.naming_case = "snake_case"
        elif "PascalCase" in rule:
            ruleset.naming_case = "PascalCase"

    # Table prefix
    table_prefixes = conventions.get("table_prefixes", {})
    if table_prefixes:
        ruleset.table_prefix = list(table_prefixes.keys())[0]

    # View prefix
    view_prefixes = conventions.get("view_prefixes", {})
    if view_prefixes:
        ruleset.view_prefix = list(view_prefixes.keys())[0]

    # Proc prefix
    proc_prefixes = conventions.get("proc_prefixes", {})
    if proc_prefixes:
        ruleset.proc_prefix = list(proc_prefixes.keys())[0]

    # Distribution
    dists = conventions.get("common_distributions", {})
    if dists:
        ruleset.default_distribution = list(dists.keys())[0]

    # Layer detection: map discovered schemas to medallion layers
    schema_patterns = conventions.get("schema_patterns", {})
    detected_layers = conventions.get("detected_layers", [])

    # Map schema names to roles
    layer_map = {
        "raw": "bronze", "bronze": "bronze", "landing": "bronze", "staging": "bronze", "stg": "bronze",
        "silver": "silver", "cleansed": "silver", "curated": "silver", "conformed": "silver",
        "gold": "gold", "analytics": "gold", "presentation": "gold", "mart": "gold", "dw": "gold",
    }

    for schema_name, pattern in schema_patterns.items():
        mapped = layer_map.get(schema_name.lower())
        if mapped == "bronze":
            ruleset.bronze_schema = schema_name
            purpose = pattern.get("purpose", "")
            if "external" in purpose:
                ruleset.bronze_object_type = "external_table"
            else:
                ruleset.bronze_object_type = "table"
        elif mapped == "silver":
            ruleset.silver_schema = schema_name
            purpose = pattern.get("purpose", "")
            if "view" in purpose:
                ruleset.silver_object_type = "view"
            else:
                ruleset.silver_object_type = "table"
        elif mapped == "gold":
            ruleset.gold_schema = schema_name
            purpose = pattern.get("purpose", "")
            if "view" in purpose:
                ruleset.gold_object_type = "view"
            else:
                ruleset.gold_object_type = "table"

    # ADF pipeline naming
    pipeline_patterns = conventions.get("pipeline_patterns", {})
    if pipeline_patterns:
        ruleset.adf_pipeline_prefix = list(pipeline_patterns.keys())[0]

    # ADLS container mapping
    containers = adls.get("containers", [])
    for c in containers:
        name = c.get("name", "").lower()
        if name in ("raw", "bronze", "landing", "source"):
            ruleset.adls_raw_container = c["name"]
            break

    # Add discovered naming rules as custom rules
    ruleset.custom_rules = conventions.get("naming_rules", [])

    logger.info("Convention ruleset built: %s naming, bronze=[%s], silver=[%s], gold=[%s], delivery=%s",
                ruleset.naming_case, ruleset.bronze_schema, ruleset.silver_schema,
                ruleset.gold_schema, ruleset.delivery_mode)

    return ruleset


def apply_conventions_to_name(name: str, ruleset: ConventionRuleset, object_type: str = "table") -> str:
    """Apply naming conventions to a generated object name."""
    # Remove existing prefixes
    for prefix in ["ext_", "vw_", "sp_", "tbl_", "dim_", "fact_"]:
        if name.lower().startswith(prefix):
            name = name[len(prefix):]
            break

    # Apply case
    if ruleset.naming_case == "snake_case":
        name = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
        name = re.sub(r'[^a-z0-9_]', '_', name)
        name = re.sub(r'_+', '_', name).strip('_')
    elif ruleset.naming_case == "PascalCase":
        parts = re.split(r'[_\s]+', name)
        name = "".join(p.capitalize() for p in parts if p)

    # Apply prefix
    if object_type == "view" and ruleset.view_prefix:
        name = f"{ruleset.view_prefix}_{name}"
    elif object_type == "procedure" and ruleset.proc_prefix:
        name = f"{ruleset.proc_prefix}_{name}"
    elif object_type == "table" and ruleset.table_prefix:
        name = f"{ruleset.table_prefix}_{name}"

    return name
