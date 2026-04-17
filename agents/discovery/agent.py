"""Discovery Agent: Scans an existing Azure data platform and builds an environment profile.

Connects to client's existing:
  - Synapse (schemas, tables, views, procs, external tables, distributions)
  - ADF (pipelines, datasets, linked services, triggers)
  - ADLS (container/folder structure, file formats)
  - Power BI (workspaces, datasets, dataflows) [optional]

Produces an EnvironmentProfile that all other agents use as context.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Optional

from shared.config import AppConfig
from shared.synapse_client import SynapseClient

logger = logging.getLogger(__name__)


class EnvironmentProfile:
    """Complete snapshot of a client's existing data platform."""

    def __init__(self):
        self.discovered_at: str = datetime.utcnow().isoformat()
        self.synapse: dict = {}
        self.adf: dict = {}
        self.adls: dict = {}
        self.powerbi: dict = {}
        self.conventions: dict = {}
        self.summary: dict = {}

    def to_dict(self) -> dict:
        return {
            "discovered_at": self.discovered_at,
            "synapse": self.synapse,
            "adf": self.adf,
            "adls": self.adls,
            "powerbi": self.powerbi,
            "conventions": self.conventions,
            "summary": self.summary,
        }


class DiscoveryAgent:
    """Scans existing Azure data platform infrastructure."""

    def __init__(self, config: AppConfig):
        self._config = config
        self._synapse = SynapseClient(config)

    def discover(self, options: dict = None) -> dict:
        """Run full discovery scan. Returns EnvironmentProfile as dict."""
        options = options or {}
        profile = EnvironmentProfile()
        logger.info("Discovery Agent: starting full environment scan")

        # 1. Synapse Discovery
        if options.get("skip_synapse") is not True:
            profile.synapse = self._discover_synapse()

        # 2. ADF Discovery
        if options.get("skip_adf") is not True:
            profile.adf = self._discover_adf()

        # 3. ADLS Discovery
        if options.get("skip_adls") is not True:
            profile.adls = self._discover_adls()

        # 4. Convention Detection
        profile.conventions = self._detect_conventions(profile)

        # 5. Summary
        profile.summary = self._build_summary(profile)

        logger.info("Discovery complete: %s", json.dumps(profile.summary))
        return profile.to_dict()

    # ── Synapse Discovery ───────────────────────────────────────

    def _discover_synapse(self) -> dict:
        """Scan Synapse for all schemas, objects, columns, distributions."""
        logger.info("Discovering Synapse objects...")
        result = {"schemas": {}, "object_count": 0, "column_count": 0}

        try:
            # Get all schemas
            schemas = self._synapse.execute_query(
                "SELECT name FROM sys.schemas WHERE name NOT IN "
                "('sys','INFORMATION_SCHEMA','guest','db_owner','db_accessadmin',"
                "'db_securityadmin','db_ddladmin','db_backupoperator','db_datareader',"
                "'db_datawriter','db_denydatareader','db_denydatawriter')"
            )

            for schema_row in schemas:
                schema_name = schema_row["name"]
                schema_data = {"tables": [], "views": [], "external_tables": [], "procedures": []}

                # Tables with distribution info
                tables = self._synapse.execute_query(
                    "SELECT t.name, t.type_desc, t.create_date, t.modify_date, "
                    "ISNULL(tdp.distribution_policy_desc, 'UNKNOWN') AS distribution "
                    "FROM sys.tables t "
                    "JOIN sys.schemas s ON t.schema_id = s.schema_id "
                    "LEFT JOIN sys.pdw_table_distribution_properties tdp ON t.object_id = tdp.object_id "
                    "WHERE s.name = ?",
                    params=(schema_name,),
                )
                for t in tables:
                    cols = self._synapse.get_columns(schema_name, t["name"])
                    schema_data["tables"].append({
                        "name": t["name"],
                        "distribution": t.get("distribution", "UNKNOWN"),
                        "created": str(t.get("create_date", "")),
                        "modified": str(t.get("modify_date", "")),
                        "columns": [{"name": c["COLUMN_NAME"], "type": c["DATA_TYPE"],
                                     "nullable": c["IS_NULLABLE"]} for c in cols],
                        "column_count": len(cols),
                    })
                    result["column_count"] += len(cols)

                # Views
                views = self._synapse.execute_query(
                    "SELECT v.name, v.create_date, v.modify_date "
                    "FROM sys.views v JOIN sys.schemas s ON v.schema_id = s.schema_id "
                    "WHERE s.name = ?",
                    params=(schema_name,),
                )
                for v in views:
                    cols = self._synapse.get_columns(schema_name, v["name"])
                    schema_data["views"].append({
                        "name": v["name"],
                        "created": str(v.get("create_date", "")),
                        "columns": [{"name": c["COLUMN_NAME"], "type": c["DATA_TYPE"]} for c in cols],
                        "column_count": len(cols),
                    })
                    result["column_count"] += len(cols)

                # External tables
                try:
                    ext_tables = self._synapse.execute_query(
                        "SELECT et.name, et.create_date "
                        "FROM sys.external_tables et "
                        "JOIN sys.schemas s ON et.schema_id = s.schema_id "
                        "WHERE s.name = ?",
                        params=(schema_name,),
                    )
                    for et in ext_tables:
                        cols = self._synapse.get_columns(schema_name, et["name"])
                        schema_data["external_tables"].append({
                            "name": et["name"],
                            "created": str(et.get("create_date", "")),
                            "columns": [{"name": c["COLUMN_NAME"], "type": c["DATA_TYPE"]} for c in cols],
                        })
                except Exception as e:
                    logger.warning("Non-critical error fetching external tables for %s: %s", schema_name, e)

                # Stored procedures
                try:
                    procs = self._synapse.execute_query(
                        "SELECT p.name, p.create_date, p.modify_date "
                        "FROM sys.procedures p JOIN sys.schemas s ON p.schema_id = s.schema_id "
                        "WHERE s.name = ?",
                        params=(schema_name,),
                    )
                    schema_data["procedures"] = [
                        {"name": p["name"], "created": str(p.get("create_date", ""))}
                        for p in procs
                    ]
                except Exception as e:
                    logger.warning("Non-critical error fetching procedures for %s: %s", schema_name, e)

                obj_count = (len(schema_data["tables"]) + len(schema_data["views"])
                             + len(schema_data["external_tables"]) + len(schema_data["procedures"]))
                if obj_count > 0:
                    result["schemas"][schema_name] = schema_data
                    result["object_count"] += obj_count

        except Exception as e:
            logger.error("Synapse discovery failed: %s", str(e)[:300])
            result["error"] = str(e)[:300]

        logger.info("Synapse: %d schemas, %d objects, %d columns",
                     len(result["schemas"]), result["object_count"], result["column_count"])
        return result

    # ── ADF Discovery ───────────────────────────────────────────

    def _discover_adf(self) -> dict:
        """Scan ADF for pipelines, datasets, linked services, triggers."""
        logger.info("Discovering ADF objects...")
        result = {"pipelines": [], "datasets": [], "linked_services": [], "triggers": [],
                  "pipeline_count": 0, "dataset_count": 0}

        try:
            from shared.adf_client import ADFClient
            adf = ADFClient()
            if not adf.is_configured:
                result["status"] = "not_configured"
                return result

            import requests
            from azure.identity import DefaultAzureCredential
            cred = DefaultAzureCredential()
            token = cred.get_token("https://management.azure.com/.default").token
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

            sub = os.environ.get("AZURE_SUBSCRIPTION_ID", "")
            rg = os.environ.get("ADF_RESOURCE_GROUP", "")
            factory = os.environ.get("ADF_NAME", "")
            base = f"https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{factory}"
            api = "?api-version=2018-06-01"

            # Pipelines
            resp = requests.get(f"{base}/pipelines{api}", headers=headers, timeout=30)
            if resp.ok:
                for pl in resp.json().get("value", []):
                    activities = pl.get("properties", {}).get("activities", [])
                    result["pipelines"].append({
                        "name": pl["name"],
                        "activity_count": len(activities),
                        "activities": [{"name": a["name"], "type": a["type"]} for a in activities],
                    })
                result["pipeline_count"] = len(result["pipelines"])

            # Datasets
            resp = requests.get(f"{base}/datasets{api}", headers=headers, timeout=30)
            if resp.ok:
                for ds in resp.json().get("value", []):
                    props = ds.get("properties", {})
                    result["datasets"].append({
                        "name": ds["name"],
                        "type": props.get("type", ""),
                        "linked_service": props.get("linkedServiceName", {}).get("referenceName", ""),
                    })
                result["dataset_count"] = len(result["datasets"])

            # Linked Services
            resp = requests.get(f"{base}/linkedservices{api}", headers=headers, timeout=30)
            if resp.ok:
                for ls in resp.json().get("value", []):
                    props = ls.get("properties", {})
                    result["linked_services"].append({
                        "name": ls["name"],
                        "type": props.get("type", ""),
                    })

            # Triggers
            resp = requests.get(f"{base}/triggers{api}", headers=headers, timeout=30)
            if resp.ok:
                for tr in resp.json().get("value", []):
                    props = tr.get("properties", {})
                    result["triggers"].append({
                        "name": tr["name"],
                        "type": props.get("type", ""),
                        "runtime_state": props.get("runtimeState", ""),
                    })

        except Exception as e:
            logger.error("ADF discovery failed: %s", str(e)[:300])
            result["error"] = str(e)[:300]

        logger.info("ADF: %d pipelines, %d datasets", result["pipeline_count"], result["dataset_count"])
        return result

    # ── ADLS Discovery ──────────────────────────────────────────

    def _discover_adls(self) -> dict:
        """Scan ADLS Gen2 for container/folder structure."""
        logger.info("Discovering ADLS structure...")
        result = {"containers": [], "container_count": 0}

        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.filedatalake import DataLakeServiceClient

            account = self._config.storage_account_name
            cred = DefaultAzureCredential()
            service = DataLakeServiceClient(
                account_url=f"https://{account}.dfs.core.windows.net",
                credential=cred,
            )

            for container in service.list_file_systems():
                container_info = {
                    "name": container.name,
                    "top_level_folders": [],
                    "file_formats": set(),
                }
                try:
                    fs_client = service.get_file_system_client(container.name)
                    for path in fs_client.get_paths(recursive=False, max_results=50):
                        if path.is_directory:
                            container_info["top_level_folders"].append(path.name)
                        else:
                            ext = path.name.rsplit(".", 1)[-1].lower() if "." in path.name else ""
                            if ext:
                                container_info["file_formats"].add(ext)
                except Exception as e:
                    logger.warning("Non-critical error listing ADLS paths: %s", e)

                container_info["file_formats"] = list(container_info["file_formats"])
                result["containers"].append(container_info)

            result["container_count"] = len(result["containers"])
        except Exception as e:
            logger.warning("ADLS discovery failed (may need azure-storage-file-datalake): %s", str(e)[:200])
            result["error"] = str(e)[:200]

        logger.info("ADLS: %d containers", result["container_count"])
        return result

    # ── Convention Detection ────────────────────────────────────

    def _detect_conventions(self, profile: EnvironmentProfile) -> dict:
        """Auto-detect naming conventions, patterns, and practices from discovered objects."""
        logger.info("Detecting naming conventions...")
        conventions = {
            "schema_patterns": {},
            "table_prefixes": {},
            "view_prefixes": {},
            "proc_prefixes": {},
            "pipeline_patterns": {},
            "common_distributions": {},
            "detected_layers": [],
            "naming_rules": [],
        }

        # Analyze Synapse object names
        all_tables = []
        all_views = []
        all_procs = []
        schema_names = list(profile.synapse.get("schemas", {}).keys())

        for schema_name, schema_data in profile.synapse.get("schemas", {}).items():
            for t in schema_data.get("tables", []):
                all_tables.append({"schema": schema_name, "name": t["name"],
                                   "distribution": t.get("distribution", "")})
            for v in schema_data.get("views", []):
                all_views.append({"schema": schema_name, "name": v["name"]})
            for p in schema_data.get("procedures", []):
                all_procs.append({"schema": schema_name, "name": p["name"]})

        # Detect table name prefixes
        table_prefixes = Counter()
        for t in all_tables:
            parts = re.split(r'[_.]', t["name"])
            if len(parts) > 1:
                table_prefixes[parts[0].lower()] += 1
        conventions["table_prefixes"] = {k: v for k, v in table_prefixes.most_common(10) if v >= 2}

        # Detect view name prefixes
        view_prefixes = Counter()
        for v in all_views:
            parts = re.split(r'[_.]', v["name"])
            if len(parts) > 1:
                view_prefixes[parts[0].lower()] += 1
        conventions["view_prefixes"] = {k: v for k, v in view_prefixes.most_common(10) if v >= 2}

        # Detect proc name prefixes
        proc_prefixes = Counter()
        for p in all_procs:
            parts = re.split(r'[_.]', p["name"])
            if len(parts) > 1:
                proc_prefixes[parts[0].lower()] += 1
        conventions["proc_prefixes"] = {k: v for k, v in proc_prefixes.most_common(10) if v >= 2}

        # Detect distribution patterns
        dist_counter = Counter(t["distribution"] for t in all_tables if t["distribution"] != "UNKNOWN")
        conventions["common_distributions"] = dict(dist_counter.most_common(5))

        # Detect medallion-like layers
        known_layers = {"raw", "bronze", "staging", "stg", "silver", "cleansed", "curated",
                        "gold", "analytics", "presentation", "mart", "dim", "fact", "dw"}
        detected = [s for s in schema_names if s.lower() in known_layers]
        conventions["detected_layers"] = detected

        # Detect schema purposes based on object patterns
        for schema_name in schema_names:
            schema_data = profile.synapse.get("schemas", {}).get(schema_name, {})
            ext_count = len(schema_data.get("external_tables", []))
            table_count = len(schema_data.get("tables", []))
            view_count = len(schema_data.get("views", []))
            proc_count = len(schema_data.get("procedures", []))

            purpose = "unknown"
            if ext_count > 0 and table_count == 0:
                purpose = "landing/raw (external tables only)"
            elif table_count > 0 and view_count == 0 and ext_count == 0:
                purpose = "staging/silver (tables, no views)"
            elif view_count > table_count:
                purpose = "presentation/gold (view-heavy)"
            elif proc_count > table_count:
                purpose = "etl/processing (proc-heavy)"
            elif table_count > 0:
                purpose = "storage (table-heavy)"

            conventions["schema_patterns"][schema_name] = {
                "purpose": purpose,
                "objects": {"tables": table_count, "views": view_count,
                            "external_tables": ext_count, "procedures": proc_count},
            }

        # Generate naming rules
        rules = []
        if conventions["view_prefixes"]:
            top_prefix = list(conventions["view_prefixes"].keys())[0]
            rules.append(f"Views use prefix '{top_prefix}_' (e.g., {top_prefix}_customer)")
        if conventions["proc_prefixes"]:
            top_prefix = list(conventions["proc_prefixes"].keys())[0]
            rules.append(f"Procedures use prefix '{top_prefix}_' (e.g., {top_prefix}_load_customer)")
        if conventions["table_prefixes"]:
            top_prefix = list(conventions["table_prefixes"].keys())[0]
            rules.append(f"Tables use prefix '{top_prefix}_' (e.g., {top_prefix}_sales)")
        if detected:
            rules.append(f"Medallion layers detected: {', '.join(detected)}")

        # Detect case convention
        snake_count = sum(1 for t in all_tables if "_" in t["name"])
        pascal_count = sum(1 for t in all_tables if re.match(r'^[A-Z][a-z]+[A-Z]', t["name"]))
        if snake_count > pascal_count:
            rules.append("Naming convention: snake_case")
        elif pascal_count > snake_count:
            rules.append("Naming convention: PascalCase")

        conventions["naming_rules"] = rules

        # ADF pipeline patterns
        pipeline_prefixes = Counter()
        for pl in profile.adf.get("pipelines", []):
            parts = pl["name"].split("_")
            if len(parts) > 1:
                pipeline_prefixes[parts[0].lower()] += 1
        conventions["pipeline_patterns"] = {k: v for k, v in pipeline_prefixes.most_common(5) if v >= 2}

        return conventions

    # ── Summary ─────────────────────────────────────────────────

    def _build_summary(self, profile: EnvironmentProfile) -> dict:
        """Build a human-readable summary of the environment."""
        syn = profile.synapse
        adf = profile.adf
        adls = profile.adls
        conv = profile.conventions

        return {
            "synapse": {
                "schemas": len(syn.get("schemas", {})),
                "total_objects": syn.get("object_count", 0),
                "total_columns": syn.get("column_count", 0),
                "detected_layers": conv.get("detected_layers", []),
            },
            "adf": {
                "pipelines": adf.get("pipeline_count", 0),
                "datasets": adf.get("dataset_count", 0),
                "linked_services": len(adf.get("linked_services", [])),
                "triggers": len(adf.get("triggers", [])),
            },
            "adls": {
                "containers": adls.get("container_count", 0),
            },
            "conventions_detected": len(conv.get("naming_rules", [])),
            "naming_rules": conv.get("naming_rules", []),
        }
