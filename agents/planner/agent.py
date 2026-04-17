"""Planner Agent: Parses stories, detects mode, generates build plans."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from shared.config import AppConfig
from shared.llm_client import LLMClient
from shared.models import (
    BuildPlan,
    ExecutionMode,
    StoryContract,
)
from shared.synapse_client import SynapseClient

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).parent / "prompts"


class PlannerAgent:
    def __init__(self, config: AppConfig):
        self._llm = LLMClient(config)
        self._synapse = SynapseClient(config)
        self._story_parser_prompt = (PROMPTS_DIR / "story_parser.txt").read_text()
        self._plan_generator_prompt = (PROMPTS_DIR / "plan_generator.txt").read_text()

    def _load_feedback(self) -> list[str]:
        """Load recent unresolved feedback to guide planning."""
        try:
            import pyodbc, os
            driver = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
            server = os.environ.get("CONFIG_DB_SERVER", os.environ.get("SOURCE_DB_SERVER", ""))
            database = os.environ.get("CONFIG_DB_NAME", os.environ.get("SOURCE_DB_NAME", ""))
            user = os.environ.get("SYNAPSE_SQL_USER", "sqladmin")
            password = os.environ.get("SYNAPSE_SQL_PASSWORD", "")
            if not server or not database:
                return []
            conn_str = (
                f"DRIVER={{{driver}}};SERVER=tcp:{server},1433;DATABASE={database};"
                f"UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=5;"
            )
            conn = pyodbc.connect(conn_str, timeout=5)
            cur = conn.cursor()
            cur.execute("SELECT TOP(10) feedback_text, category, affected_object FROM config.feedback WHERE status = 'open' ORDER BY created_at DESC")
            rows = [f"[{r[1]}] {r[0]} (object: {r[2] or 'general'})" for r in cur.fetchall()]
            conn.close()
            return rows
        except Exception as e:
            logger.debug("Non-critical error loading feedback: %s", e)
            return []

    def run(self, story_input: str | dict) -> BuildPlan:
        """Full planner pipeline: parse → detect mode → generate build plan."""
        logger.info("Planner Agent started")

        # Step 1: Parse story into structured contract
        story = self._parse_story(story_input)
        logger.info("Parsed story: %s (tables: %s)", story.story_id, story.source_tables)

        # Step 2: Detect execution mode
        mode, catalog_context = self._detect_mode(story)
        logger.info("Detected mode: %s", mode.value)

        # Step 3: Generate build plan
        build_plan = self._generate_build_plan(story, mode, catalog_context)
        logger.info(
            "Build plan: %d steps, risk=%s",
            len(build_plan.execution_order),
            build_plan.risk_level,
        )

        return build_plan

    def _parse_story(self, story_input: str | dict) -> StoryContract:
        """Parse free-text or structured story into StoryContract."""
        if isinstance(story_input, dict):
            return StoryContract(**story_input)

        result = self._llm.chat_json(
            system_prompt=self._story_parser_prompt,
            user_prompt=story_input,
        )
        return StoryContract(**result)

    def _detect_mode(self, story: StoryContract) -> tuple[ExecutionMode, dict]:
        """Query Synapse metadata to determine greenfield/brownfield/partial/hybrid.

        Optimized: single connectivity check + single combined query for bronze/silver.
        If Synapse is unreachable (paused/firewalled), defaults to greenfield in <5s.
        """
        catalog_context = {
            "bronze_tables": [],
            "silver_tables": [],
            "existing_columns": {},
            "approved_joins": [],
            "glossary": [],
        }

        source_table_names = [t.split(".")[-1] for t in story.source_tables]

        # FAST connectivity check (5s timeout) — if Synapse is paused, skip all queries
        synapse_reachable = False
        try:
            import pyodbc, os
            driver = os.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
            fast_conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER=tcp:{self._synapse._endpoint},1433;"
                f"DATABASE={self._synapse._database};"
                f"UID={self._synapse._user};"
                f"PWD={self._synapse._password};"
                f"Encrypt=yes;TrustServerCertificate=no;"
                f"Connection Timeout=5;"
            )
            conn = pyodbc.connect(fast_conn_str, timeout=5)
            conn.cursor().execute("SELECT 1")
            conn.close()
            synapse_reachable = True
            logger.info("Synapse connectivity check: OK")
        except Exception as e:
            logger.warning("Synapse unreachable (pool likely paused): %s — defaulting to greenfield", str(e)[:100])

        if not synapse_reachable:
            logger.info("Skipping all Synapse metadata queries — mode=greenfield")
            return ExecutionMode.GREENFIELD, catalog_context

        # COMBINED bronze+silver check in a single query
        try:
            combined_sql = """
            SELECT TABLE_SCHEMA AS layer, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA IN ('bronze', 'silver')
            UNION ALL
            SELECT s.name AS layer, o.name AS TABLE_NAME
            FROM sys.objects o JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name IN ('bronze', 'silver') AND o.type IN ('U', 'V')
            AND o.name NOT IN (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN ('bronze', 'silver'))
            """
            rows = self._synapse.execute_query(combined_sql)
            for r in rows:
                layer = r.get("layer", "").lower()
                name = r.get("TABLE_NAME", "")
                if layer == "bronze":
                    catalog_context["bronze_tables"].append(name)
                elif layer == "silver":
                    catalog_context["silver_tables"].append(name)
            logger.info("Found %d bronze, %d silver objects",
                        len(catalog_context["bronze_tables"]), len(catalog_context["silver_tables"]))
        except Exception as e:
            logger.warning("Bronze/silver check failed: %s", e)

        # Catalog queries (non-critical, best-effort)
        try:
            catalog_context["approved_joins"] = self._synapse.execute_query(
                "SELECT left_schema, left_table, left_column, right_schema, right_table, right_column, join_type, cardinality "
                "FROM [catalog].[approved_joins] WHERE is_validated = 1"
            )
        except Exception as e:
            logger.debug("Non-critical error loading approved joins: %s", e)

        try:
            if source_table_names:
                placeholders = ','.join('?' for _ in source_table_names)
                catalog_context["glossary"] = self._synapse.execute_query(
                    f"SELECT business_term, physical_schema, physical_table, physical_column, description "
                    f"FROM [catalog].[business_glossary] WHERE physical_table IN ({placeholders})",
                    params=tuple(source_table_names),
                )
        except Exception as e:
            logger.debug("Non-critical error loading business glossary: %s", e)

        # Mode decision
        has_bronze = any(
            self._table_name_matches(t, catalog_context["bronze_tables"])
            for t in source_table_names
        )
        has_silver = any(
            self._table_name_matches(t, catalog_context["silver_tables"])
            for t in source_table_names
        )

        all_bronze = all(
            self._table_name_matches(t, catalog_context["bronze_tables"])
            for t in source_table_names
        )
        all_silver = all(
            self._table_name_matches(t, catalog_context["silver_tables"])
            for t in source_table_names
        )

        if all_bronze and all_silver:
            mode = ExecutionMode.BROWNFIELD
        elif all_bronze and not all_silver:
            mode = ExecutionMode.PARTIAL
        elif has_bronze or has_silver:
            mode = ExecutionMode.HYBRID
        elif not has_bronze and has_silver:
            mode = ExecutionMode.ANOMALY
        else:
            mode = ExecutionMode.GREENFIELD

        return mode, catalog_context

    def _generate_build_plan(
        self, story: StoryContract, mode: ExecutionMode, catalog_context: dict
    ) -> BuildPlan:
        """Use LLM to generate the build plan based on story + mode + catalog."""
        feedback_items = self._load_feedback()
        user_prompt = json.dumps(
            {
                "story": story.model_dump(),
                "mode": mode.value,
                "catalog_context": catalog_context,
                "user_feedback": feedback_items,
            },
            indent=2,
            default=str,
        )

        try:
            result = self._llm.chat_json(
                system_prompt=self._plan_generator_prompt,
                user_prompt=user_prompt,
                max_tokens=4096,
            )

            # Filter out steps with invalid layers (e.g. adf_pipeline as layer)
            valid_layers = {"bronze", "silver", "gold"}
            if "execution_order" in result:
                result["execution_order"] = [
                    step for step in result["execution_order"]
                    if step.get("layer", "").lower() in valid_layers
                ]

            return BuildPlan(**result)
        except Exception as e:
            logger.error("LLM plan generation failed: %s — using template fallback", e)
            return self._template_fallback_plan(story, mode)

    def _template_fallback_plan(self, story: StoryContract, mode: ExecutionMode) -> BuildPlan:
        """Generate a basic plan without LLM when AI is unavailable/slow."""
        logger.info("Using template fallback for %s (mode=%s)", story.story_id, mode.value)
        steps = []
        step_num = 1

        source_schema = story.source_tables[0].split(".")[0] if story.source_tables else "sales"
        for tbl in story.source_tables:
            parts = tbl.split(".")
            schema = parts[0] if len(parts) > 1 else source_schema
            table = parts[-1]

            if mode in (ExecutionMode.GREENFIELD, ExecutionMode.PARTIAL):
                steps.append({
                    "step": step_num, "layer": "bronze", "action": "create",
                    "artifact_type": "external_table",
                    "object_name": f"[bronze].[ext_{schema}_{table}]",
                    "source": {"system": schema, "schema_name": schema, "table": table},
                    "columns": [], "logic_summary": f"External table for {schema}.{table}",
                    "load_pattern": "full", "depends_on": [],
                })
                step_num += 1

        if mode in (ExecutionMode.GREENFIELD, ExecutionMode.PARTIAL):
            domain = story.story_id.replace("STORY-", "").lower()
            steps.append({
                "step": step_num, "layer": "silver", "action": "create",
                "artifact_type": "table",
                "object_name": f"[silver].[{domain}_summary]",
                "columns": [], "logic_summary": "Cleansed and joined data",
                "depends_on": list(range(1, step_num)),
            })
            step_num += 1

        view_name = story.target_view_name or f"vw_{story.story_id.replace('STORY-','').lower()}_analysis"
        steps.append({
            "step": step_num, "layer": "gold", "action": "create",
            "artifact_type": "view",
            "object_name": f"[gold].[{view_name}]",
            "columns": [], "logic_summary": story.business_objective or "Gold presentation view",
            "depends_on": [step_num - 1],
        })

        risk = "low" if mode == ExecutionMode.BROWNFIELD else "medium"
        return BuildPlan(
            story_id=story.story_id,
            mode=mode,
            risk_level=risk,
            execution_order=steps,
            validation_requirements=[
                {"check_type": "row_count", "layer": "gold", "table": f"gold.{view_name}"},
            ],
        )

    @staticmethod
    def _table_name_matches(source_table: str, existing_tables: list[str]) -> bool:
        """Check if a source table has a corresponding object in a layer."""
        normalized = source_table.lower().replace(".", "_")
        return any(
            normalized in existing.lower() or existing.lower() in normalized
            for existing in existing_tables
        )
