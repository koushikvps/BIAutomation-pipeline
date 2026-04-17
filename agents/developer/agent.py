"""Developer Agent: Generates SQL artifacts and ADF pipeline JSON from build plans."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from shared.config import AppConfig
from shared.llm_client import LLMClient
from shared.models import (
    ArtifactBundle,
    ArtifactType,
    BuildPlan,
    BuildStep,
    GeneratedArtifact,
    Layer,
)

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).parent / "prompts"
TEMPLATES_DIR = Path(__file__).parent / "templates"


class DeveloperAgent:
    def __init__(self, config: AppConfig):
        self._config = config
        self._llm = LLMClient(config)
        self._sql_prompt = (PROMPTS_DIR / "sql_generator.txt").read_text()
        self._adf_prompt = (PROMPTS_DIR / "adf_generator.txt").read_text()
        self._source_columns_cache: dict[str, list[dict]] = {}

    def _get_source_columns(self, schema_name: str, table_name: str) -> list[dict] | None:
        """Query source DB for full column list."""
        cache_key = f"{schema_name}.{table_name}"
        if cache_key in self._source_columns_cache:
            return self._source_columns_cache[cache_key]
        try:
            from shared.synapse_client import SynapseClient
            import pyodbc, os
            driver = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER=tcp:{self._config.source_db_server},1433;"
                f"DATABASE={self._config.source_db_name};"
                f"UID={os.environ.get('SYNAPSE_SQL_USER', 'sqladmin')};"
                f"PWD={os.environ.get('SYNAPSE_SQL_PASSWORD', '')};"
                f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
            )
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, ORDINAL_POSITION "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                "ORDER BY ORDINAL_POSITION",
                (schema_name, table_name),
            )
            cols = []
            type_map = {"int": "int", "nvarchar": "nvarchar", "varchar": "varchar",
                        "date": "date", "datetime": "datetime", "datetime2": "datetime2",
                        "decimal": "decimal", "bit": "bit", "bigint": "bigint", "float": "float"}
            for row in cursor.fetchall():
                col_name, data_type, max_len, nullable, _ = row
                dt = data_type.lower()
                if dt in ("nvarchar", "varchar") and max_len:
                    dt = f"{dt}({max_len})"
                elif dt == "decimal":
                    dt = "decimal(18,2)"
                cols.append({"name": col_name, "data_type": dt, "is_nullable": nullable == "YES"})
            conn.close()
            self._source_columns_cache[cache_key] = cols
            logger.info("Fetched %d columns for %s.%s from source DB", len(cols), schema_name, table_name)
            return cols
        except Exception as e:
            logger.warning("Could not fetch source columns for %s.%s: %s", schema_name, table_name, e)
            return None

    def _load_semantic_definitions(self) -> dict[str, str]:
        """Load semantic definitions from Config DB so gold views use consistent business terms."""
        try:
            import pyodbc, os
            driver = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
            server = os.environ.get("CONFIG_DB_SERVER", os.environ.get("SOURCE_DB_SERVER", ""))
            database = os.environ.get("CONFIG_DB_NAME", os.environ.get("SOURCE_DB_NAME", ""))
            user = os.environ.get("SYNAPSE_SQL_USER", "sqladmin")
            password = os.environ.get("SYNAPSE_SQL_PASSWORD", "")
            if not server or not database:
                return {}
            conn_str = (
                f"DRIVER={{{driver}}};SERVER=tcp:{server},1433;DATABASE={database};"
                f"UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=5;"
            )
            conn = pyodbc.connect(conn_str, timeout=5)
            cur = conn.cursor()
            cur.execute("SELECT term, definition FROM config.semantic_definitions")
            defs = {r[0]: r[1] for r in cur.fetchall()}
            conn.close()
            logger.info("Loaded %d semantic definitions", len(defs))
            return defs
        except Exception as e:
            logger.debug("No semantic definitions loaded: %s", e)
            return {}

    def run(self, build_plan: BuildPlan) -> ArtifactBundle:
        logger.info("Developer Agent started for %s (%d steps)",
                     build_plan.story_id, len(build_plan.execution_order))

        self._semantic_defs = self._load_semantic_definitions()
        artifacts: list[GeneratedArtifact] = []
        seen_objects = set()

        for step in build_plan.execution_order:
            obj_key = (step.layer.value, step.object_name.lower())
            if step.layer == Layer.SILVER and step.artifact_type == ArtifactType.STORED_PROCEDURE:
                view_key = (step.layer.value, self._to_silver_name(step).lower())
                if view_key in seen_objects:
                    logger.info("Skipping redundant step %d: %s", step.step, step.object_name)
                    continue

            if obj_key in seen_objects:
                logger.info("Skipping duplicate step %d: %s", step.step, step.object_name)
                continue

            logger.info("Generating step %d: %s [%s]", step.step, step.object_name, step.artifact_type)
            artifact = self._generate_artifact(step, build_plan)
            if artifact:
                artifacts.append(artifact)
                seen_objects.add((artifact.layer.value, artifact.object_name.lower()))

        # Generate ADF pipeline JSON for Bronze ingestion
        bronze_steps = [s for s in build_plan.execution_order if s.layer == Layer.BRONZE]
        if bronze_steps:
            adf_artifact = self._build_adf_pipeline(bronze_steps, build_plan)
            if adf_artifact:
                artifacts.append(adf_artifact)

        bundle = ArtifactBundle(story_id=build_plan.story_id, artifacts=artifacts)
        logger.info("Developer Agent completed: %d artifacts generated", len(artifacts))
        return bundle

    def _generate_artifact(self, step: BuildStep, plan: BuildPlan) -> GeneratedArtifact:
        if step.layer == Layer.BRONZE and step.artifact_type == ArtifactType.EXTERNAL_TABLE:
            content = self._build_external_table(step, plan)
            return GeneratedArtifact(
                step=step.step, artifact_type=ArtifactType.EXTERNAL_TABLE,
                object_name=step.object_name, layer=step.layer,
                file_name=f"bronze/{self._clean_name(step.object_name)}.sql",
                content=content,
            )

        if step.layer == Layer.SILVER:
            if step.artifact_type == ArtifactType.STORED_PROCEDURE and not step.columns:
                return None
            content = self._build_silver_table(step, plan)
            table_name = self._to_silver_name(step)
            return GeneratedArtifact(
                step=step.step, artifact_type=ArtifactType.TABLE,
                object_name=table_name, layer=step.layer,
                file_name=f"silver/{self._clean_name(table_name)}.sql",
                content=content,
            )

        if step.layer == Layer.GOLD:
            content = self._build_gold_view(step, plan)
            return GeneratedArtifact(
                step=step.step, artifact_type=ArtifactType.VIEW,
                object_name=step.object_name, layer=step.layer,
                file_name=f"gold/{self._clean_name(step.object_name)}.sql",
                content=content,
            )

        content = self._generate_sql_via_llm(step, plan)
        file_ext = "json" if step.artifact_type == ArtifactType.ADF_PIPELINE else "sql"
        return GeneratedArtifact(
            step=step.step, artifact_type=step.artifact_type,
            object_name=step.object_name, layer=step.layer,
            file_name=f"{step.layer.value}/{self._clean_name(step.object_name)}.{file_ext}",
            content=content,
        )

    def _build_external_table(self, step: BuildStep, plan: BuildPlan) -> str:
        columns = step.columns
        if step.source and getattr(step.source, "schema_name", None) and getattr(step.source, "table", None):
            source_cols = self._get_source_columns(step.source.schema_name, step.source.table)
            if source_cols:
                columns = source_cols

        cols = []
        for c in columns:
            nullable = "NULL" if c.get("is_nullable", True) else "NOT NULL"
            cols.append(f"    [{c['name']}] {c['data_type']} {nullable}")
        col_def = ",\n".join(cols)
        table_name = step.object_name.replace("[", "").replace("]", "").split(".")[-1]

        return f"""IF OBJECT_ID('{step.object_name.replace('[','').replace(']','')}', 'U') IS NOT NULL
    DROP EXTERNAL TABLE {step.object_name};
GO

CREATE EXTERNAL TABLE {step.object_name} (
{col_def}
)
WITH (
    LOCATION = '{table_name}/',
    DATA_SOURCE = [BronzeDataSource],
    FILE_FORMAT = [ParquetFileFormat]
);"""

    def _build_silver_table(self, step: BuildStep, plan: BuildPlan) -> str:
        if not step.columns:
            silver_table_step = next(
                (s for s in plan.execution_order
                 if s.layer == Layer.SILVER and s.artifact_type == ArtifactType.TABLE and s.columns),
                None,
            )
            if silver_table_step:
                step = silver_table_step

        table_name = self._to_silver_name(step)
        bronze_steps = [s for s in plan.execution_order if s.layer == Layer.BRONZE]
        if not bronze_steps:
            return f"-- ERROR: No bronze steps found for {table_name}"

        join_map = {}
        for i, bs in enumerate(bronze_steps):
            alias = f"t{i+1}"
            tname = bs.object_name.replace("[", "").replace("]", "").split(".")[-1]
            join_map[tname.lower()] = (bs.object_name, alias)
            # Strip common prefixes to allow flexible join lookups
            for prefix in ["SalesDB_", "sales_", "dbo_"]:
                if tname.lower().startswith(prefix.lower()):
                    short = tname[len(prefix):]
                    join_map[short.lower()] = (bs.object_name, alias)
                    break

        primary = bronze_steps[0]

        has_aggregations = any(
            any(agg in c["name"] for agg in ["Total", "Count", "Sum", "Avg"])
            for c in step.columns if not c["name"].startswith("_")
        )

        select_cols = []
        group_cols = []
        for c in step.columns:
            name = c["name"]
            if name.startswith("_"):
                continue
            if has_aggregations:
                if name == "TotalRevenue":
                    select_cols.append("    SUM(t2.[LineTotal]) AS [TotalRevenue]")
                elif name == "OrderCount":
                    select_cols.append("    COUNT(DISTINCT t1.[OrderId]) AS [OrderCount]")
                elif name == "TotalUnitsSold":
                    select_cols.append("    SUM(t2.[Quantity]) AS [TotalUnitsSold]")
                else:
                    for bs in bronze_steps:
                        for bc in bs.columns:
                            if bc["name"].lower() == name.lower():
                                tname = bs.object_name.replace("[", "").replace("]", "").split(".")[-1]
                                _, alias = join_map.get(tname.lower(), (bs.object_name, "t1"))
                                select_cols.append(f"    {alias}.[{bc['name']}]")
                                group_cols.append(f"    {alias}.[{bc['name']}]")
                                break
            else:
                for bs in bronze_steps:
                    for bc in bs.columns:
                        if bc["name"].lower() == name.lower():
                            tname = bs.object_name.replace("[", "").replace("]", "").split(".")[-1]
                            _, alias = join_map.get(tname.lower(), (bs.object_name, "t1"))
                            select_cols.append(f"    {alias}.[{bc['name']}]")
                            break

        select_str = ",\n".join(select_cols)

        joins = []
        join_pairs = [
            ("OrderHeader", "OrderDetail", "OrderId"),
            ("OrderDetail", "Product", "ProductId"),
            ("OrderHeader", "Customer", "CustomerId"),
        ]
        for left, right, key in join_pairs:
            left_info = join_map.get(left.lower())
            right_info = join_map.get(right.lower())
            if left_info and right_info:
                joins.append(f"INNER JOIN {right_info[0]} {right_info[1]} ON {left_info[1]}.[{key}] = {right_info[1]}.[{key}]")

        join_str = "\n".join(joins)

        where_parts = []
        for rule in step.business_rules:
            r = rule.lower()
            if "cancel" in r:
                where_parts.append("    t1.[Status] <> 'Cancelled'")
            if "12 month" in r or "last 12" in r:
                where_parts.append("    t1.[OrderDate] >= DATEADD(MONTH, -12, CAST(GETUTCDATE() AS DATE))")
        where_str = "\nAND ".join(where_parts) if where_parts else "1=1"

        group_str = ""
        if has_aggregations and group_cols:
            group_str = f"\nGROUP BY\n" + ",\n".join(group_cols)

        return f"""IF OBJECT_ID('{table_name.replace('[','').replace(']','')}', 'U') IS NOT NULL
    DROP TABLE {table_name};

CREATE TABLE {table_name}
WITH (
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT
{select_str},
    GETUTCDATE() AS [_ingested_at],
    '{plan.story_id}' AS [_source_story]
FROM {primary.object_name} t1
{join_str}
WHERE {where_str}{group_str};"""

    def _build_gold_view(self, step: BuildStep, plan: BuildPlan) -> str:
        silver_step = next(
            (s for s in plan.execution_order
             if s.layer == Layer.SILVER and s.columns),
            None,
        )
        if not silver_step:
            return f"-- ERROR: No silver step found for {step.object_name}"

        silver_name = self._to_silver_name(silver_step)

        select_cols = []
        for c in silver_step.columns:
            name = c["name"]
            if name.startswith("_"):
                continue
            if name == "OrderDate":
                select_cols.append(f"    [{name}] AS [DailySalesDate]")
            elif name == "Region":
                select_cols.append(f"    [{name}] AS [SalesRegion]")
            elif name == "Category":
                select_cols.append(f"    [{name}] AS [ProductCategory]")
            else:
                select_cols.append(f"    [{name}]")

        select_str = ",\n".join(select_cols)

        # Inject semantic definitions as column-level comments
        semantic_notes = []
        for col_line in select_cols:
            for term, defn in getattr(self, '_semantic_defs', {}).items():
                if term.lower().replace(" ", "") in col_line.lower().replace(" ", ""):
                    semantic_notes.append(f"    -- {term}: {defn}")
        semantic_block = "\n".join(semantic_notes)
        semantic_comment = f"\n    -- Semantic Definitions:\n{semantic_block}" if semantic_notes else ""

        view_fqn = step.object_name.replace("[", "").replace("]", "")
        return f"""IF OBJECT_ID('{view_fqn}', 'V') IS NOT NULL
    DROP VIEW {step.object_name};
GO

CREATE VIEW {step.object_name}
AS
/*
    Business Purpose: {step.logic_summary or 'Gold presentation view'}
    Source Story: {plan.story_id}{semantic_comment}
*/
SELECT
{select_str}
FROM {silver_name};"""

    def _build_adf_pipeline(self, bronze_steps: list[BuildStep], plan: BuildPlan) -> GeneratedArtifact | None:
        """Generate an ADF pipeline JSON template for Bronze ingestion (SQL -> ADLS Parquet).
        Supports incremental load via watermark columns (modified_date, updated_at, etc.)."""
        source_system = "SalesDB"
        if bronze_steps and bronze_steps[0].source and bronze_steps[0].source.system:
            source_system = bronze_steps[0].source.system

        watermark_candidates = ["modified_date", "updated_at", "last_modified", "changed_date", "order_date", "created_at"]

        copy_activities = []
        for step in bronze_steps:
            src = step.source
            if not src or not src.schema_name or not src.table:
                continue
            table_name = step.object_name.replace("[", "").replace("]", "").split(".")[-1]

            # Detect watermark column from source columns
            watermark_col = None
            if hasattr(src, "columns") and src.columns:
                col_names = [c.lower() if isinstance(c, str) else c.get("name", "").lower() for c in src.columns]
                for wm in watermark_candidates:
                    if wm in col_names:
                        watermark_col = wm
                        break

            if watermark_col:
                query = (
                    f"SELECT * FROM [{src.schema_name}].[{src.table}] "
                    f"WHERE [{watermark_col}] > '@{{pipeline().parameters.lastWatermark}}' "
                    f"AND [{watermark_col}] <= '@{{pipeline().parameters.currentWatermark}}'"
                )
            else:
                query = f"SELECT * FROM [{src.schema_name}].[{src.table}]"

            activity = {
                "name": f"Copy_{src.schema_name}_{src.table}",
                "type": "Copy",
                "inputs": [{
                    "referenceName": f"SqlMI_{src.schema_name}_{src.table}",
                    "type": "DatasetReference",
                }],
                "outputs": [{
                    "referenceName": f"ADLS_Parquet_{table_name}",
                    "type": "DatasetReference",
                }],
                "typeProperties": {
                    "source": {
                        "type": "SqlSource",
                        "sqlReaderQuery": query,
                    },
                    "sink": {
                        "type": "ParquetSink",
                        "storeSettings": {
                            "type": "AzureBlobFSWriteSettings",
                        },
                        "formatSettings": {
                            "type": "ParquetWriteSettings",
                        },
                    },
                    "enableStaging": False,
                },
                "policy": {
                    "timeout": "0.02:00:00",
                    "retry": 2,
                    "retryIntervalInSeconds": 30,
                },
            }
            copy_activities.append(activity)

        if not copy_activities:
            return None

        pipeline_json = {
            "name": f"pl_bronze_{plan.story_id.replace('-', '_')}",
            "properties": {
                "description": f"Bronze ingestion pipeline for {plan.story_id}",
                "activities": copy_activities,
                "annotations": [plan.story_id, "auto-generated"],
                "parameters": {
                    "triggerDate": {"type": "String", "defaultValue": "@utcNow()"},
                    "lastWatermark": {"type": "String", "defaultValue": "1900-01-01"},
                    "currentWatermark": {"type": "String", "defaultValue": "@utcNow()"},
                },
                "folder": {"name": "bronze/auto-generated"},
            },
        }

        import json as _json
        content = _json.dumps(pipeline_json, indent=2)
        pipeline_name = f"pl_bronze_{plan.story_id.replace('-', '_')}"

        return GeneratedArtifact(
            step=0,
            artifact_type=ArtifactType.ADF_PIPELINE,
            object_name=pipeline_name,
            layer=Layer.BRONZE,
            file_name=f"adf/{pipeline_name}.json",
            content=content,
        )

    def _to_silver_name(self, step: BuildStep) -> str:
        clean = step.object_name.replace("[", "").replace("]", "")
        if "." in clean:
            _, obj = clean.split(".", 1)
        else:
            obj = clean
        obj = obj.replace("usp_load_", "").replace("vw_", "")
        return f"[silver].[{obj}]"

    def _clean_name(self, name: str) -> str:
        return name.replace("[", "").replace("]", "").replace(".", "_")

    def _generate_sql_via_llm(self, step: BuildStep, plan: BuildPlan) -> str:
        template = self._load_template(step.artifact_type)
        user_prompt = json.dumps(
            {"step": step.model_dump(), "story_id": plan.story_id,
             "mode": plan.mode.value, "template_reference": template},
            indent=2, default=str,
        )
        return self._llm.chat(
            system_prompt=self._sql_prompt, user_prompt=user_prompt,
            temperature=0.0, max_tokens=4096,
        )

    def _generate_adf_pipeline(self, step: BuildStep, plan: BuildPlan) -> str:
        user_prompt = json.dumps(
            {"step": step.model_dump(), "story_id": plan.story_id},
            indent=2, default=str,
        )
        return self._llm.chat(
            system_prompt=self._adf_prompt, user_prompt=user_prompt,
            temperature=0.0, max_tokens=4096,
        )

    def _load_template(self, artifact_type: ArtifactType) -> str:
        template_map = {
            ArtifactType.EXTERNAL_TABLE: "bronze/external_table_parquet.sql",
            ArtifactType.TABLE: "silver/create_table.sql",
            ArtifactType.STORED_PROCEDURE: "silver/proc_incremental_merge.sql",
            ArtifactType.VIEW: "gold/view_aggregated.sql",
        }
        template_path = TEMPLATES_DIR / template_map.get(artifact_type, "")
        if template_path.exists():
            return template_path.read_text()
        return ""
