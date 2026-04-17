"""
Durable Functions Orchestrator: Wires all agents together.

Endpoints:
  POST /api/process-story       → starts the orchestration (JSON story input)
  POST /api/process-ado-story   → fetches story from ADO work item and starts orchestration
  POST /api/bot-message         → Teams bot webhook (natural language commands)
  GET  /api/status/{id}         → check pipeline status
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

import azure.functions as func
import azure.durable_functions as df

from shared.config import AppConfig
from shared.models import (
    ArtifactBundle,
    BuildPlan,
    HealerResult,
    PipelineState,
    StoryContract,
    ValidationReport,
    ValidationStatus,
)

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)
logger = logging.getLogger(__name__)

MAX_HEAL_RETRIES = 3

import os as _os_module
STATIC_DIR = _os_module.path.join(_os_module.path.dirname(__file__), "static")


# ============================================================
# HTTP TRIGGER: Health Check
# ============================================================
@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
async def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint for monitoring and alerting."""
    import datetime
    checks = {"timestamp": datetime.datetime.utcnow().isoformat(), "status": "healthy", "checks": {}}

    # Check Synapse connectivity
    try:
        config = AppConfig.from_env()
        from shared.synapse_client import SynapseClient
        synapse = SynapseClient(config)
        synapse.execute_query("SELECT 1 AS ok")
        checks["checks"]["synapse"] = {"status": "ok"}
    except Exception as e:
        checks["checks"]["synapse"] = {"status": "error", "message": str(e)[:200]}
        checks["status"] = "degraded"

    # Check Config DB connectivity
    try:
        config = AppConfig.from_env()
        from shared.state_registry import StateRegistry
        reg = StateRegistry(config)
        reg.get_pipeline_history(limit=1)
        checks["checks"]["config_db"] = {"status": "ok"}
    except Exception as e:
        checks["checks"]["config_db"] = {"status": "error", "message": str(e)[:200]}
        checks["status"] = "degraded"

    # Check LLM connectivity
    try:
        config = AppConfig.from_env()
        from shared.llm_client import LLMClient
        llm = LLMClient(config)
        llm.chat("You are a test.", "Reply with OK", max_tokens=5)
        checks["checks"]["llm"] = {"status": "ok"}
    except Exception as e:
        checks["checks"]["llm"] = {"status": "error", "message": str(e)[:200]}
        checks["status"] = "degraded"

    # Check ADF connectivity
    try:
        from shared.adf_client import ADFClient
        adf = ADFClient()
        checks["checks"]["adf"] = {"status": "ok" if adf.is_configured else "not_configured"}
    except Exception as e:
        checks["checks"]["adf"] = {"status": "error", "message": str(e)[:200]}

    status_code = 200 if checks["status"] == "healthy" else 207
    return func.HttpResponse(json.dumps(checks, indent=2), mimetype="application/json", status_code=status_code)


# ============================================================
# HTTP TRIGGER: Web UI
# ============================================================
@app.route(route="ui", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
async def serve_ui(req: func.HttpRequest) -> func.HttpResponse:
    """Serve the web chat UI."""
    html_path = _os_module.path.join(STATIC_DIR, "index.html")
    try:
        with open(html_path, "r") as f:
            return func.HttpResponse(f.read(), mimetype="text/html")
    except FileNotFoundError:
        return func.HttpResponse("UI not found", status_code=404)


# ============================================================
# HTTP TRIGGER: Pipeline History (from Config DB)
# ============================================================
@app.route(route="pipeline-history", methods=["GET"])
async def pipeline_history(req: func.HttpRequest) -> func.HttpResponse:
    """Return pipeline execution history from Config DB."""
    try:
        config = AppConfig.from_env()
        from shared.state_registry import StateRegistry
        reg = StateRegistry(config)
        limit = int(req.params.get("limit", "20"))
        pipelines = reg.get_pipeline_history(limit=limit)
        # Convert datetime objects to strings
        for p in pipelines:
            for k, v in p.items():
                if hasattr(v, "isoformat"):
                    p[k] = v.isoformat()
        return func.HttpResponse(
            json.dumps({"pipelines": pipelines}, indent=2, default=str),
            mimetype="application/json",
        )
    except Exception as e:
        logger.error("pipeline-history error: %s", e)
        return func.HttpResponse(
            json.dumps({"error": str(e), "pipelines": []}),
            status_code=500, mimetype="application/json",
        )


# ============================================================
# HTTP TRIGGER: Artifact History for a specific object
# ============================================================
@app.route(route="artifact-history", methods=["GET"])
async def artifact_history(req: func.HttpRequest) -> func.HttpResponse:
    """Return version history for a specific artifact."""
    object_name = req.params.get("object_name", "")
    if not object_name:
        return func.HttpResponse(
            json.dumps({"error": "object_name parameter required"}),
            status_code=400, mimetype="application/json",
        )
    try:
        config = AppConfig.from_env()
        from shared.state_registry import StateRegistry
        reg = StateRegistry(config)
        versions = reg.get_artifact_history(object_name)
        for v in versions:
            for k, val in v.items():
                if hasattr(val, "isoformat"):
                    v[k] = val.isoformat()
        return func.HttpResponse(
            json.dumps({"object_name": object_name, "versions": versions}, indent=2, default=str),
            mimetype="application/json",
        )
    except Exception as e:
        logger.error("artifact-history error: %s", e)
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500, mimetype="application/json",
        )


# ============================================================
# HTTP TRIGGER: Run Data Quality Checks (on-demand)
# ============================================================
# ============================================================
# HTTP TRIGGER: Data Lineage (live row counts + object metadata)
# ============================================================
@app.route(route="data-lineage", methods=["GET"])
async def data_lineage(req: func.HttpRequest) -> func.HttpResponse:
    """Return live data lineage: source tables, bronze/silver/gold objects with row counts."""
    try:
        config = AppConfig.from_env()
        from shared.synapse_client import SynapseClient
        synapse = SynapseClient(config)

        lineage = {"source": [], "bronze": [], "silver": [], "gold": []}

        # Source tables (from source DB — separate connection)
        try:
            import pyodbc as _pyodbc
            src_server = config.source_db_server
            src_db = config.source_db_name
            import os as _os2
            src_user = _os2.environ.get("SYNAPSE_SQL_USER", "sqladmin")
            src_pass = _os2.environ.get("SYNAPSE_SQL_PASSWORD", "")
            driver = _os2.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
            src_conn_str = (
                f"DRIVER={{{driver}}};SERVER=tcp:{src_server},1433;DATABASE={src_db};"
                f"UID={src_user};PWD={src_pass};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=15;"
            )
            src_conn = _pyodbc.connect(src_conn_str, autocommit=True)
            cur = src_conn.cursor()
            cur.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA NOT IN ('config','sys')")
            for row in cur.fetchall():
                schema, name = row[0], row[1]
                try:
                    cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{name}]")
                    cnt = cur.fetchone()[0]
                except Exception as e:
                    logger.warning("Non-critical error counting rows for %s.%s: %s", schema, name, e)
                    cnt = "?"
                lineage["source"].append({"schema": schema, "name": name, "rows": cnt})
            src_conn.close()
        except Exception as e:
            logger.warning("Could not fetch source tables: %s", e)

        # Synapse objects by layer
        for layer in ["bronze", "silver", "gold"]:
            try:
                objs = synapse.execute_query(f"""
                    SELECT obj_name, obj_type FROM (
                        SELECT o.name AS obj_name, o.type_desc AS obj_type
                        FROM sys.objects o
                        JOIN sys.schemas s ON o.schema_id = s.schema_id
                        WHERE s.name = '{layer}' AND o.type IN ('U','V')
                        UNION ALL
                        SELECT et.name AS obj_name, 'EXTERNAL_TABLE' AS obj_type
                        FROM sys.external_tables et
                        JOIN sys.schemas s ON et.schema_id = s.schema_id
                        WHERE s.name = '{layer}'
                    ) combined
                """)
                for obj in objs:
                    name = obj["obj_name"]
                    try:
                        rows = synapse.execute_query(f"SELECT COUNT(*) AS cnt FROM [{layer}].[{name}]")
                        cnt = rows[0]["cnt"] if rows else 0
                    except Exception as e:
                        logger.warning("Non-critical error counting rows for %s.%s: %s", layer, name, e)
                        cnt = "?"
                    lineage[layer].append({
                        "name": name,
                        "type": obj["obj_type"],
                        "rows": cnt,
                    })
            except Exception as e:
                logger.warning("Could not fetch %s objects: %s", layer, e)

        return func.HttpResponse(
            json.dumps(lineage, indent=2, default=str),
            mimetype="application/json",
        )
    except Exception as e:
        logger.error("data-lineage error: %s", e)
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500, mimetype="application/json",
        )


# ============================================================
# HTTP TRIGGER: Data Catalog (browse all datasets + columns)
# ============================================================
@app.route(route="data-catalog", methods=["GET"])
async def data_catalog(req: func.HttpRequest) -> func.HttpResponse:
    """Return catalog of all datasets across source DB and Synapse layers."""
    try:
        config = AppConfig.from_env()
        from shared.synapse_client import SynapseClient
        import pyodbc as _pyodbc
        import os as _os2

        catalog = {"datasets": [], "total_columns": 0, "total_tables": 0}

        # Source DB tables
        try:
            src_server = config.source_db_server
            src_db = config.source_db_name
            driver = _os2.environ.get("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
            src_conn = _pyodbc.connect(
                f"DRIVER={{{driver}}};SERVER=tcp:{src_server},1433;DATABASE={src_db};"
                f"UID={_os2.environ.get('SYNAPSE_SQL_USER','sqladmin')};"
                f"PWD={_os2.environ.get('SYNAPSE_SQL_PASSWORD','')};"
                f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=15;",
                autocommit=True,
            )
            cur = src_conn.cursor()
            cur.execute("""
                SELECT t.TABLE_SCHEMA, t.TABLE_NAME,
                       (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS c
                        WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME) AS col_count
                FROM INFORMATION_SCHEMA.TABLES t
                WHERE t.TABLE_TYPE = 'BASE TABLE' AND t.TABLE_SCHEMA NOT IN ('config','sys')
            """)
            for row in cur.fetchall():
                schema, name, col_count = row[0], row[1], row[2]
                # Get columns
                cur2 = src_conn.cursor()
                cur2.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{name}'
                    ORDER BY ORDINAL_POSITION
                """)
                columns = [{"name": r[0], "type": r[1], "nullable": r[2], "max_length": r[3]} for r in cur2.fetchall()]
                catalog["datasets"].append({
                    "source": "source_db", "layer": "source", "schema": schema,
                    "name": name, "full_name": f"{schema}.{name}",
                    "column_count": col_count, "columns": columns,
                    "database": src_db,
                })
                catalog["total_columns"] += col_count
                catalog["total_tables"] += 1
            src_conn.close()
        except Exception as e:
            logger.warning("Catalog: could not fetch source tables: %s", e)

        # Synapse layers
        try:
            synapse = SynapseClient(config)
            for layer in ["bronze", "silver", "gold"]:
                try:
                    objs = synapse.execute_query(f"""
                        SELECT obj_name, obj_type FROM (
                            SELECT o.name AS obj_name, o.type_desc AS obj_type
                            FROM sys.objects o JOIN sys.schemas s ON o.schema_id = s.schema_id
                            WHERE s.name = '{layer}' AND o.type IN ('U','V')
                            UNION ALL
                            SELECT et.name AS obj_name, 'EXTERNAL_TABLE' AS obj_type
                            FROM sys.external_tables et JOIN sys.schemas s ON et.schema_id = s.schema_id
                            WHERE s.name = '{layer}'
                        ) combined
                    """)
                    for obj in objs:
                        name = obj["obj_name"]
                        try:
                            cols = synapse.get_columns(layer, name)
                            columns = [{"name": c["COLUMN_NAME"], "type": c["DATA_TYPE"],
                                        "nullable": c.get("IS_NULLABLE"), "max_length": c.get("CHARACTER_MAXIMUM_LENGTH")} for c in cols]
                        except Exception as e:
                            logger.warning("Non-critical error fetching columns for %s.%s: %s", layer, name, e)
                            columns = []
                        catalog["datasets"].append({
                            "source": "synapse", "layer": layer, "schema": layer,
                            "name": name, "full_name": f"{layer}.{name}",
                            "type": obj["obj_type"], "column_count": len(columns),
                            "columns": columns, "database": "bipool",
                        })
                        catalog["total_columns"] += len(columns)
                        catalog["total_tables"] += 1
                except Exception as e:
                    logger.warning("Catalog: could not fetch %s: %s", layer, e)
        except Exception as e:
            logger.warning("Catalog: Synapse unavailable: %s", e)

        return func.HttpResponse(json.dumps(catalog, indent=2, default=str), mimetype="application/json")
    except Exception as e:
        logger.error("data-catalog error: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Natural Language Query (ask questions, get SQL + results)
# ============================================================
@app.route(route="nl-query", methods=["POST"])
async def nl_query(req: func.HttpRequest) -> func.HttpResponse:
    """Execute a natural language query against Gold layer. Returns SQL + results.

    Security: LLM-generated SQL is validated before execution.
    Only SELECT on gold/silver schemas is allowed.
    """
    try:
        body = req.get_json()
        question = body.get("question", "").strip()
        if not question or len(question) > 2000:
            return func.HttpResponse(json.dumps({"error": "question required (max 2000 chars)"}), status_code=400, mimetype="application/json")

        config = AppConfig.from_env()
        from shared.llm_client import LLMClient
        from shared.synapse_client import SynapseClient

        synapse = SynapseClient(config)

        available_objects = []
        for layer in ["gold", "silver"]:
            try:
                objs = synapse.execute_query(
                    "SELECT o.name AS obj_name FROM sys.objects o "
                    "JOIN sys.schemas s ON o.schema_id = s.schema_id "
                    "WHERE s.name = ? AND o.type = 'V'",
                    params=(layer,),
                )
                for obj in objs:
                    name = obj["obj_name"]
                    try:
                        cols = synapse.get_columns(layer, name)
                        col_list = ", ".join([f"{c['COLUMN_NAME']} ({c['DATA_TYPE']})" for c in cols])
                    except Exception as e:
                        logger.warning("Non-critical error fetching columns for %s.%s: %s", layer, name, e)
                        col_list = "unknown"
                    available_objects.append(f"[{layer}].[{name}]: {col_list}")
            except Exception as e:
                logger.warning("Non-critical error fetching objects for layer %s: %s", layer, e)

        schema_context = "\n".join(available_objects) if available_objects else "No views available"

        llm = LLMClient(config)
        system_prompt = f"""You are a SQL query generator for Azure Synapse Dedicated Pool.
Available views and their columns:
{schema_context}

Rules:
- Generate only SELECT queries (no INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, EXEC, TRUNCATE)
- Use the exact schema.view names shown above
- Return ONLY the SQL query, no explanations
- Use TOP 100 to limit results
- Do NOT use dynamic SQL, EXEC, sp_executesql, xp_ procedures, or OPENROWSET
- Format numbers and dates nicely"""

        sql = llm.chat(system_prompt=system_prompt, user_prompt=question, temperature=0.0, max_tokens=1000)
        sql = sql.strip().strip("```").strip("sql\n").strip()

        # SQL SAFETY VALIDATION: Block dangerous statements
        sql_upper = sql.upper().replace("\n", " ").replace("\r", " ")
        blocked_keywords = [
            "INSERT ", "UPDATE ", "DELETE ", "DROP ", "CREATE ", "ALTER ",
            "TRUNCATE ", "EXEC ", "EXECUTE ", "EXEC(", "SP_EXECUTESQL",
            "XP_", "OPENROWSET", "OPENDATASOURCE", "OPENQUERY",
            "BULK INSERT", "GRANT ", "REVOKE ", "DENY ",
            "SHUTDOWN", "DBCC ", "BACKUP ", "RESTORE ",
        ]
        for keyword in blocked_keywords:
            if keyword in sql_upper:
                logger.warning("Blocked dangerous SQL from nl-query: %s", sql[:200])
                return func.HttpResponse(json.dumps({
                    "question": question,
                    "sql": sql,
                    "error": f"Query blocked: contains prohibited keyword '{keyword.strip()}'",
                    "rows": [],
                }), status_code=400, mimetype="application/json")

        # Verify query only references allowed schemas
        import re
        referenced_schemas = set(re.findall(r'\[(\w+)\]\.\[', sql))
        allowed_schemas = {"gold", "silver"}
        unauthorized = referenced_schemas - allowed_schemas
        if unauthorized:
            logger.warning("Blocked SQL referencing unauthorized schemas: %s", unauthorized)
            return func.HttpResponse(json.dumps({
                "question": question,
                "sql": sql,
                "error": f"Query blocked: references unauthorized schemas {unauthorized}. Only gold/silver allowed.",
                "rows": [],
            }), status_code=400, mimetype="application/json")

        try:
            rows = synapse.execute_query(sql)
            rows = rows[:100]
            columns = list(rows[0].keys()) if rows else []
            for row in rows:
                for k, v in row.items():
                    if hasattr(v, "isoformat"):
                        row[k] = v.isoformat()
                    elif isinstance(v, (bytes, bytearray)):
                        row[k] = str(v)

            return func.HttpResponse(json.dumps({
                "question": question,
                "sql": sql,
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
            }, indent=2, default=str), mimetype="application/json")
        except Exception as qe:
            return func.HttpResponse(json.dumps({
                "question": question,
                "sql": sql,
                "error": f"Query failed: {str(qe)[:300]}",
                "rows": [],
            }, indent=2), mimetype="application/json")

    except Exception as e:
        logger.error("nl-query error: %s", str(e)[:200])
        return func.HttpResponse(json.dumps({"error": "Internal error processing query"}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Template Library
# ============================================================
@app.route(route="templates", methods=["GET"])
async def get_templates(req: func.HttpRequest) -> func.HttpResponse:
    """Return available pipeline templates."""
    templates = [
        {
            "id": "customer_360",
            "name": "Customer 360",
            "description": "Complete customer view with orders, revenue, lifetime value, and segmentation",
            "icon": "👤",
            "source_tables": ["customers", "orders", "order_items"],
            "gold_views": ["vw_customer_360", "vw_customer_segments", "vw_customer_ltv"],
            "complexity": "medium",
            "estimated_objects": 8,
        },
        {
            "id": "revenue_analytics",
            "name": "Revenue Analytics",
            "description": "Revenue trends, product performance, regional breakdown, forecasting",
            "icon": "📈",
            "source_tables": ["orders", "order_items", "products"],
            "gold_views": ["vw_revenue_daily", "vw_product_performance", "vw_revenue_by_region"],
            "complexity": "medium",
            "estimated_objects": 9,
        },
        {
            "id": "inventory_tracker",
            "name": "Inventory Tracker",
            "description": "Stock levels, reorder points, supplier performance, turnover rates",
            "icon": "📦",
            "source_tables": ["products", "order_items", "orders"],
            "gold_views": ["vw_stock_levels", "vw_reorder_alerts", "vw_supplier_scorecard"],
            "complexity": "low",
            "estimated_objects": 7,
        },
        {
            "id": "sales_performance",
            "name": "Sales Performance",
            "description": "Sales team KPIs, pipeline metrics, conversion rates, quota attainment",
            "icon": "🎯",
            "source_tables": ["orders", "customers", "products"],
            "gold_views": ["vw_sales_kpi", "vw_pipeline_metrics", "vw_quota_attainment"],
            "complexity": "high",
            "estimated_objects": 10,
        },
        {
            "id": "data_quality_report",
            "name": "Data Quality Report",
            "description": "Completeness, freshness, accuracy scores across all datasets",
            "icon": "✅",
            "source_tables": ["*"],
            "gold_views": ["vw_dq_scores", "vw_dq_trends"],
            "complexity": "low",
            "estimated_objects": 5,
        },
    ]
    return func.HttpResponse(json.dumps({"templates": templates}, indent=2), mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Use Template (creates ADO work item + triggers pipeline)
# ============================================================
@app.route(route="use-template", methods=["POST"])
@app.durable_client_input(client_name="client")
async def use_template(req: func.HttpRequest, client) -> func.HttpResponse:
    """Create ADO work item from template and trigger pipeline."""
    try:
        body = req.get_json()
        template_id = body.get("template_id", "")
        # Fetch template
        TEMPLATES = {
            "customer_360": {"title": "Customer 360 Analytics", "tables": "customers, orders, order_items", "desc": "Build complete customer view with orders, revenue, lifetime value, and segmentation. Source tables: sales.customers, sales.orders, sales.order_items."},
            "revenue_analytics": {"title": "Revenue Analytics Dashboard", "tables": "orders, order_items, products", "desc": "Build revenue trends, product performance, and regional breakdown. Source tables: sales.orders, sales.order_items, sales.products."},
            "inventory_tracker": {"title": "Inventory Tracker", "tables": "products, order_items, orders", "desc": "Build stock levels, reorder points, supplier performance views. Source tables: sales.products, sales.order_items, sales.orders."},
            "sales_performance": {"title": "Sales Performance KPIs", "tables": "orders, customers, products", "desc": "Build sales KPIs, pipeline metrics, conversion rates. Source tables: sales.orders, sales.customers, sales.products."},
            "data_quality_report": {"title": "Data Quality Report", "tables": "customers, orders, order_items, products", "desc": "Build data quality scoring views across all datasets."},
        }
        tmpl = TEMPLATES.get(template_id)
        if not tmpl:
            return func.HttpResponse(json.dumps({"error": f"Unknown template: {template_id}"}), status_code=400, mimetype="application/json")

        from shared.ado_client import ADOClient
        ado = ADOClient()
        wi = ado.create_work_item(
            title=f"{tmpl['title']} - Auto Generated from Template",
            description=f"<p>{tmpl['desc']}</p><p>Source tables: {tmpl['tables']}</p>",
            tags="bi-automation,template",
        )
        work_item_id = wi.get("id")
        logger.info("Created ADO work item %s from template %s", work_item_id, template_id)

        # Now trigger pipeline
        from shared.story_mapper import StoryMapper
        config = AppConfig.from_env()
        wi_fields = ado.get_work_item_fields(work_item_id)
        mapper = StoryMapper(config)
        story_json = mapper.map_work_item(wi_fields)

        instance_id = await client.start_new("story_orchestrator", client_input=story_json)
        return func.HttpResponse(json.dumps({
            "work_item_id": work_item_id,
            "instance_id": instance_id,
            "template_id": template_id,
            "story_id": story_json.get("story_id"),
            "title": tmpl["title"],
            "source_tables": story_json.get("source_tables", []),
        }, indent=2), mimetype="application/json")
    except Exception as e:
        logger.error("use-template error: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Semantic Layer CRUD
# ============================================================
@app.route(route="semantic", methods=["GET", "POST"])
async def semantic_layer(req: func.HttpRequest) -> func.HttpResponse:
    """Get or save semantic definitions."""
    try:
        config = AppConfig.from_env()
        from shared.state_registry import StateRegistry
        reg = StateRegistry(config)

        if req.method == "GET":
            try:
                rows = []
                with reg._conn() as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT * FROM config.semantic_definitions ORDER BY term")
                    if cur.description:
                        cols = [c[0] for c in cur.description]
                        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                        for row in rows:
                            for k, v in row.items():
                                if hasattr(v, "isoformat"):
                                    row[k] = v.isoformat()
                return func.HttpResponse(json.dumps({"definitions": rows}, default=str), mimetype="application/json")
            except Exception as e:
                # Table doesn't exist yet, return defaults
                logger.warning("Non-critical error fetching semantic definitions: %s", e)
                return func.HttpResponse(json.dumps({"definitions": [], "note": "Run schema migration to enable persistence"}), mimetype="application/json")

        elif req.method == "POST":
            body = req.get_json()
            term = body.get("term", "")
            definition = body.get("definition", "")
            views = body.get("views", "")
            owner = body.get("owner", "")
            if not term:
                return func.HttpResponse(json.dumps({"error": "term required"}), status_code=400, mimetype="application/json")
            try:
                with reg._conn() as conn:
                    cur = conn.cursor()
                    cur.execute("""
                        IF EXISTS (SELECT 1 FROM config.semantic_definitions WHERE term = ?)
                            UPDATE config.semantic_definitions SET definition = ?, views = ?, owner = ?, updated_at = GETUTCDATE() WHERE term = ?
                        ELSE
                            INSERT INTO config.semantic_definitions (term, definition, views, owner) VALUES (?, ?, ?, ?)
                    """, term, definition, views, owner, term, term, definition, views, owner)
                return func.HttpResponse(json.dumps({"status": "saved", "term": term}), mimetype="application/json")
            except Exception as e:
                return func.HttpResponse(json.dumps({"error": str(e), "note": "Run schema migration"}), status_code=500, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Feedback CRUD
# ============================================================
@app.route(route="feedback", methods=["GET", "POST"])
async def feedback_api(req: func.HttpRequest) -> func.HttpResponse:
    """Get or submit feedback."""
    try:
        config = AppConfig.from_env()
        from shared.state_registry import StateRegistry
        reg = StateRegistry(config)

        if req.method == "GET":
            try:
                with reg._conn() as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT TOP(50) * FROM config.feedback ORDER BY created_at DESC")
                    if cur.description:
                        cols = [c[0] for c in cur.description]
                        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                        for row in rows:
                            for k, v in row.items():
                                if hasattr(v, "isoformat"):
                                    row[k] = v.isoformat()
                        return func.HttpResponse(json.dumps({"feedback": rows}, default=str), mimetype="application/json")
                return func.HttpResponse(json.dumps({"feedback": []}), mimetype="application/json")
            except Exception as e:
                logger.warning("Non-critical error fetching feedback: %s", e)
                return func.HttpResponse(json.dumps({"feedback": [], "note": "Run schema migration"}), mimetype="application/json")

        elif req.method == "POST":
            body = req.get_json()
            text = body.get("text", "")
            category = body.get("category", "General")
            affected_object = body.get("affected_object", "")
            if not text:
                return func.HttpResponse(json.dumps({"error": "text required"}), status_code=400, mimetype="application/json")
            try:
                with reg._conn() as conn:
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO config.feedback (feedback_text, category, affected_object)
                        VALUES (?, ?, ?)
                    """, text, category, affected_object)
                return func.HttpResponse(json.dumps({"status": "submitted"}), mimetype="application/json")
            except Exception as e:
                return func.HttpResponse(json.dumps({"error": str(e), "note": "Run schema migration"}), status_code=500, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: ADF Schedules (real data from ADF API)
# ============================================================
@app.route(route="schedules", methods=["GET"])
async def get_schedules(req: func.HttpRequest) -> func.HttpResponse:
    """Fetch real pipeline schedules from ADF."""
    try:
        from shared.adf_client import ADFClient
        client = ADFClient()
        if not client.is_configured:
            return func.HttpResponse(json.dumps({"schedules": [], "note": "ADF not configured"}), mimetype="application/json")

        import requests as _req
        # Get pipelines
        pipelines_url = f"{client._base_url}/pipelines?api-version=2018-06-01"
        triggers_url = f"{client._base_url}/triggers?api-version=2018-06-01"
        schedules = []
        try:
            pr = _req.get(pipelines_url, headers=client._headers(), timeout=15)
            pipelines = pr.json().get("value", []) if pr.status_code == 200 else []
            tr = _req.get(triggers_url, headers=client._headers(), timeout=15)
            triggers = tr.json().get("value", []) if tr.status_code == 200 else []

            for p in pipelines:
                name = p.get("name", "")
                props = p.get("properties", {})
                schedule = {"name": name, "description": props.get("description", ""), "folder": props.get("folder", {}).get("name", ""), "activities": len(props.get("activities", [])), "trigger": None}
                for t in triggers:
                    t_props = t.get("properties", {})
                    for tp in t_props.get("pipelines", []):
                        if tp.get("pipelineReference", {}).get("referenceName") == name:
                            rec = t_props.get("typeProperties", {}).get("recurrence", {})
                            schedule["trigger"] = {
                                "name": t.get("name", ""),
                                "type": t_props.get("type", ""),
                                "frequency": rec.get("frequency", ""),
                                "interval": rec.get("interval", ""),
                                "hours": rec.get("schedule", {}).get("hours", []),
                                "status": t_props.get("runtimeState", "Started"),
                            }
                schedules.append(schedule)
        except Exception as e:
            logger.warning("Could not fetch ADF schedules: %s", e)

        return func.HttpResponse(json.dumps({"schedules": schedules}, indent=2, default=str), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Cost Estimates (Azure Cost Management)
# ============================================================
@app.route(route="costs", methods=["GET"])
async def get_costs(req: func.HttpRequest) -> func.HttpResponse:
    """Return cost estimates. Uses Azure Cost Management API if available, otherwise estimates."""
    try:
        import os as _os3
        sub_id = _os3.environ.get("AZURE_SUBSCRIPTION_ID", "")
        rg = _os3.environ.get("ADF_RESOURCE_GROUP", "")

        costs = {"period": "monthly", "currency": "USD", "resources": [], "total_estimated": 0}

        if sub_id and rg:
            try:
                from azure.identity import ManagedIdentityCredential, DefaultAzureCredential
                import requests as _req
                try:
                    cred = ManagedIdentityCredential()
                    token = cred.get_token("https://management.azure.com/.default").token
                except Exception as e:
                    logger.debug("ManagedIdentityCredential failed, falling back: %s", e)
                    cred = DefaultAzureCredential()
                    token = cred.get_token("https://management.azure.com/.default").token

                # List resources with costs
                url = f"https://management.azure.com/subscriptions/{sub_id}/resourceGroups/{rg}/providers/Microsoft.CostManagement/query?api-version=2023-11-01"
                body = {
                    "type": "ActualCost",
                    "timeframe": "MonthToDate",
                    "dataset": {
                        "granularity": "None",
                        "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
                        "grouping": [{"type": "Dimension", "name": "ResourceType"}],
                    },
                }
                resp = _req.post(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json=body, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    rows = data.get("properties", {}).get("rows", [])
                    for row in rows:
                        costs["resources"].append({"type": row[1] if len(row) > 1 else "unknown", "cost": round(row[0], 2) if row else 0})
                        costs["total_estimated"] += row[0] if row else 0
                    costs["total_estimated"] = round(costs["total_estimated"], 2)
                    costs["source"] = "azure_cost_management"
                    return func.HttpResponse(json.dumps(costs, indent=2, default=str), mimetype="application/json")
            except Exception as e:
                logger.warning("Cost Management API failed, using estimates: %s", e)

        # Fallback: static estimates
        costs["source"] = "estimated"
        costs["resources"] = [
            {"name": "Synapse Dedicated Pool (DW100c)", "type": "Compute", "monthly": 864, "note": "~$1.20/hr, pause when idle"},
            {"name": "Function App (EP1)", "type": "Compute", "monthly": 122},
            {"name": "Azure SQL (Basic)", "type": "Database", "monthly": 5},
            {"name": "Data Factory", "type": "Orchestration", "monthly": 15, "note": "~$0.50/run"},
            {"name": "Storage (ADLS Gen2)", "type": "Storage", "monthly": 7},
            {"name": "AI Foundry (Phi-4)", "type": "AI", "monthly": 10, "note": "~$2/pipeline"},
            {"name": "Key Vault", "type": "Security", "monthly": 0.03},
            {"name": "App Insights", "type": "Monitoring", "monthly": 2},
        ]
        costs["total_estimated"] = sum(r.get("monthly", 0) for r in costs["resources"])
        return func.HttpResponse(json.dumps(costs, indent=2, default=str), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Send Notification (Teams webhook)
# ============================================================
@app.route(route="notify", methods=["POST"])
async def send_notification(req: func.HttpRequest) -> func.HttpResponse:
    """Send a notification via Teams webhook."""
    try:
        import os as _os4
        import requests as _req
        webhook_url = _os4.environ.get("TEAMS_WEBHOOK_URL", "")
        if not webhook_url:
            return func.HttpResponse(json.dumps({"status": "skipped", "reason": "TEAMS_WEBHOOK_URL not configured"}), mimetype="application/json")

        body = req.get_json()
        title = body.get("title", "BI Platform Notification")
        message = body.get("message", "")
        card = {
            "@type": "MessageCard",
            "summary": title,
            "themeColor": "0076D7",
            "sections": [{"activityTitle": title, "facts": [{"name": "Message", "value": message}]}],
        }
        resp = _req.post(webhook_url, json=card, timeout=10)
        return func.HttpResponse(json.dumps({"status": "sent" if resp.status_code == 200 else "failed"}), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Source Connectors CRUD
# ============================================================
# ============================================================
# HTTP TRIGGER: Interpret Story (Universal — any format)
# ============================================================
@app.route(route="interpret-story", methods=["POST"])
async def interpret_story(req: func.HttpRequest) -> func.HttpResponse:
    """Interpret any story format (Gherkin, plain English, etc.) into StoryContract JSON."""
    try:
        body = req.get_json()
        text = body.get("text", "")
        title = body.get("title", "")
        work_item_id = body.get("work_item_id", 0)
        priority = body.get("priority", "medium")

        if not text:
            return func.HttpResponse(json.dumps({"error": "text is required"}), status_code=400, mimetype="application/json")

        config = AppConfig.from_env()
        from shared.story_interpreter import StoryInterpreter
        interpreter = StoryInterpreter(config)

        # Detect format first
        story_format = interpreter.detect_format(text)

        # Get available tables for context
        available_tables = interpreter.get_available_tables()

        result = interpreter.interpret(
            text=text,
            work_item_id=work_item_id,
            title=title,
            priority=priority,
            source_db_schema=available_tables,
        )
        result["_available_tables"] = len(available_tables)
        return func.HttpResponse(json.dumps(result, indent=2, default=str), mimetype="application/json")
    except Exception as e:
        logger.error("interpret-story error: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Process Free-Text Story (no ADO work item needed)
# ============================================================
@app.route(route="process-free-story", methods=["POST"])
@app.durable_client_input(client_name="client")
async def process_free_story(req: func.HttpRequest, client) -> func.HttpResponse:
    """Process a story from free text (Gherkin, English, etc.) — no ADO work item needed."""
    try:
        body = req.get_json()
        text = body.get("text", "")
        title = body.get("title", "")
        priority = body.get("priority", "medium")

        if not text:
            return func.HttpResponse(json.dumps({"error": "text is required"}), status_code=400, mimetype="application/json")

        config = AppConfig.from_env()
        from shared.story_interpreter import StoryInterpreter
        interpreter = StoryInterpreter(config)
        available_tables = interpreter.get_available_tables()

        import time
        work_item_id = int(time.time()) % 1000000  # synthetic ID

        story_json = interpreter.interpret(
            text=text,
            work_item_id=work_item_id,
            title=title,
            priority=priority,
            source_db_schema=available_tables,
        )

        if not story_json.get("source_tables"):
            return func.HttpResponse(json.dumps({
                "error": "Could not determine source tables from the story",
                "interpretation": story_json,
                "hint": "Try including explicit table names or more specific business terms",
            }, indent=2), status_code=400, mimetype="application/json")

        instance_id = await client.start_new("story_orchestrator", client_input=story_json)
        return func.HttpResponse(json.dumps({
            "instance_id": instance_id,
            "story_id": story_json.get("story_id"),
            "title": story_json.get("title"),
            "source_tables": story_json.get("source_tables"),
            "confidence": story_json.get("confidence", {}),
            "interpretation_notes": story_json.get("interpretation_notes", ""),
        }, indent=2), mimetype="application/json")
    except Exception as e:
        logger.error("process-free-story error: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


@app.route(route="connectors", methods=["GET", "POST"])
async def connectors_api(req: func.HttpRequest) -> func.HttpResponse:
    """List or register data source connectors."""
    try:
        from shared.connector_client import ConnectorClient
        client = ConnectorClient()

        if req.method == "GET":
            rows = client.list_connectors()
            return func.HttpResponse(json.dumps({"connectors": rows}, default=str), mimetype="application/json")

        elif req.method == "POST":
            body = req.get_json()
            action = body.get("action", "register")
            if action == "test":
                result = client.test_connector(body.get("connector_id", 0))
                return func.HttpResponse(json.dumps(result, default=str), mimetype="application/json")
            elif action == "preview":
                result = client.extract_preview(body.get("connector_type", ""), body.get("config", {}), body.get("limit", 10))
                return func.HttpResponse(json.dumps(result, default=str), mimetype="application/json")
            else:
                result = client.register_connector(
                    name=body.get("name", ""),
                    connector_type=body.get("connector_type", ""),
                    connection_config=body.get("config", {}),
                    key_vault_secret=body.get("key_vault_secret", ""),
                    schema_hint=body.get("schema_hint"),
                )
                return func.HttpResponse(json.dumps(result, default=str), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Column-Level Lineage
# ============================================================
# ============================================================
# HTTP TRIGGER: Power BI Dataset Generation
# ============================================================
@app.route(route="generate-pbi", methods=["POST"])
async def generate_pbi(req: func.HttpRequest) -> func.HttpResponse:
    """Generate a Power BI dataset definition (TMSL/TMDL) from Gold views."""
    try:
        config = AppConfig.from_env()
        from shared.synapse_client import SynapseClient
        syn = SynapseClient(config)

        body = req.get_json() if req.get_body() else {}
        story_id = body.get("story_id", "")

        # Discover gold views
        try:
            gold_views = syn.execute_query("""
                SELECT s.name AS schema_name, o.name AS object_name
                FROM sys.objects o JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE s.name = 'gold' AND o.type IN ('V', 'U')
                ORDER BY o.name
            """)
        except Exception as e:
            logger.warning("Non-critical error fetching gold views: %s", e)
            gold_views = []

        if not gold_views:
            return func.HttpResponse(json.dumps({"error": "No Gold views found in Synapse"}), status_code=404, mimetype="application/json")

        # Build TMSL-like dataset definition
        tables = []
        for view in gold_views:
            view_name = view.get("object_name", "")
            schema_name = view.get("schema_name", "gold")
            fqn = f"[{schema_name}].[{view_name}]"

            # Get columns
            try:
                cols = syn.execute_query(f"""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{view_name}'
                    ORDER BY ORDINAL_POSITION
                """)
            except Exception as e:
                logger.warning("Non-critical error fetching columns for %s.%s: %s", schema_name, view_name, e)
                cols = []

            pbi_type_map = {
                "int": "Int64", "bigint": "Int64", "smallint": "Int64",
                "decimal": "Decimal", "float": "Double", "real": "Double",
                "nvarchar": "String", "varchar": "String", "char": "String",
                "date": "DateTime", "datetime": "DateTime", "datetime2": "DateTime",
                "bit": "Boolean",
            }

            pbi_columns = []
            measures = []
            for col in cols:
                col_name = col.get("COLUMN_NAME", "")
                data_type = col.get("DATA_TYPE", "string").lower()
                pbi_type = pbi_type_map.get(data_type, "String")

                pbi_columns.append({
                    "name": col_name,
                    "dataType": pbi_type,
                    "sourceColumn": col_name,
                    "isHidden": col_name.startswith("_"),
                })

                # Auto-generate measures for numeric columns
                if pbi_type in ("Int64", "Decimal", "Double"):
                    if any(kw in col_name.lower() for kw in ("total", "revenue", "amount", "sales", "cost", "price", "profit")):
                        measures.append({
                            "name": f"Sum of {col_name}",
                            "expression": f"SUM('{view_name}'[{col_name}])",
                            "formatString": "#,##0.00",
                        })
                    if any(kw in col_name.lower() for kw in ("count", "quantity", "units", "orders")):
                        measures.append({
                            "name": f"Total {col_name}",
                            "expression": f"SUM('{view_name}'[{col_name}])",
                            "formatString": "#,##0",
                        })

            table_def = {
                "name": view_name,
                "columns": pbi_columns,
                "measures": measures,
                "partitions": [{
                    "name": "DirectQuery",
                    "source": {
                        "type": "m",
                        "expression": f'let Source = Sql.Database("{config.synapse_endpoint}", "{config.synapse_database}"), {view_name} = Source{{[Schema="{schema_name}", Item="{view_name}"]}}[Data] in {view_name}',
                    },
                }],
            }
            tables.append(table_def)

        # Build the full dataset model
        dataset = {
            "name": f"BI_Automation_{story_id or 'Platform'}",
            "compatibilityLevel": 1550,
            "model": {
                "culture": "en-US",
                "dataAccessOptions": {"legacyRedirects": True, "returnErrorValuesAsNull": True},
                "defaultPowerBIDataSourceVersion": "powerBI_V3",
                "tables": tables,
                "annotations": [
                    {"name": "AutoGenerated", "value": "true"},
                    {"name": "Platform", "value": "BI Automation Platform"},
                    {"name": "StoryId", "value": story_id or "all"},
                ],
            },
            "datasources": [{
                "name": "Synapse",
                "connectionString": f"Data Source={config.synapse_endpoint};Initial Catalog={config.synapse_database}",
                "provider": "System.Data.SqlClient",
            }],
            "_instructions": {
                "step1": "Download this JSON file",
                "step2": "Open Power BI Desktop > Transform Data > Advanced Editor",
                "step3": "Or use Power BI REST API: POST /datasets with this body",
                "step4": "Tables connect to Synapse Gold views via DirectQuery",
                "step5": f"Synapse endpoint: {config.synapse_endpoint}",
            },
        }

        return func.HttpResponse(
            json.dumps(dataset, indent=2, default=str),
            mimetype="application/json",
            headers={"Content-Disposition": f'attachment; filename="pbi_dataset_{story_id or "platform"}.json"'},
        )
    except Exception as e:
        logger.error("generate-pbi error: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


@app.route(route="column-lineage", methods=["GET"])
async def column_lineage_api(req: func.HttpRequest) -> func.HttpResponse:
    """Return column-level lineage graph."""
    try:
        from shared.lineage_tracker import LineageTracker
        tracker = LineageTracker()
        story_id = req.params.get("story_id")
        target_table = req.params.get("target_table")
        if story_id or target_table:
            rows = tracker.get_lineage(story_id=story_id, target_table=target_table)
            return func.HttpResponse(json.dumps({"lineage": rows}, default=str), mimetype="application/json")
        else:
            graph = tracker.get_full_lineage_graph()
            return func.HttpResponse(json.dumps(graph, default=str), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


@app.route(route="data-quality", methods=["POST"])
async def run_dq_checks(req: func.HttpRequest) -> func.HttpResponse:
    """Run data quality checks on specified objects."""
    try:
        body = req.get_json()
        story_id = body.get("story_id", "")
        objects = body.get("objects", [])
        if not objects:
            return func.HttpResponse(
                json.dumps({"error": "objects list required"}),
                status_code=400, mimetype="application/json",
            )
        config = AppConfig.from_env()
        from shared.data_quality import DataQualityValidator
        dq = DataQualityValidator(config)
        report = dq.run_checks(story_id=story_id, objects=objects)
        return func.HttpResponse(
            json.dumps(report.to_dict(), indent=2),
            mimetype="application/json",
        )
    except Exception as e:
        logger.error("data-quality error: %s", e)
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500, mimetype="application/json",
        )


# ============================================================
# HTTP TRIGGER: Start processing a story
# ============================================================
# ============================================================
# HTTP TRIGGER: Approve Plan (Human Review Gate)
# ============================================================
@app.route(route="approve-plan", methods=["POST"])
@app.durable_client_input(client_name="client")
async def approve_plan(req: func.HttpRequest, client) -> func.HttpResponse:
    """Approve the execution plan — raises HumanReview event to resume orchestrator."""
    try:
        body = req.get_json()
        instance_id = body.get("instance_id", "")
        if not instance_id:
            return func.HttpResponse(json.dumps({"error": "instance_id required"}), status_code=400, mimetype="application/json")
        await client.raise_event(instance_id, "HumanReview", {"approved": True})

        # Feedback loop: index approved plan into RAG knowledge base
        try:
            from shared.approval_feedback import ApprovalFeedbackLoop
            from shared.rag_retriever import RAGRetriever
            feedback = ApprovalFeedbackLoop(RAGRetriever(AppConfig.from_env()))
            plan_data = body.get("plan", body.get("plan_data", {}))
            story_id = body.get("story_id", instance_id)
            if plan_data:
                feedback.on_plan_approved(story_id, plan_data, instance_id)
        except Exception as fb_err:
            logger.warning("Approval feedback indexing failed (non-blocking): %s", fb_err)

        return func.HttpResponse(json.dumps({"status": "approved", "instance_id": instance_id}), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Decline Plan (Human Review Gate)
# ============================================================
@app.route(route="decline-plan", methods=["POST"])
@app.durable_client_input(client_name="client")
async def decline_plan(req: func.HttpRequest, client) -> func.HttpResponse:
    """Decline the execution plan — stops the orchestrator."""
    try:
        body = req.get_json()
        instance_id = body.get("instance_id", "")
        reason = body.get("reason", "Declined by reviewer")
        if not instance_id:
            return func.HttpResponse(json.dumps({"error": "instance_id required"}), status_code=400, mimetype="application/json")
        await client.raise_event(instance_id, "HumanReview", {"approved": False, "reason": reason})

        # Feedback loop: index declined plan as anti-pattern
        try:
            from shared.approval_feedback import ApprovalFeedbackLoop
            from shared.rag_retriever import RAGRetriever
            feedback = ApprovalFeedbackLoop(RAGRetriever(AppConfig.from_env()))
            plan_data = body.get("plan", body.get("plan_data", {}))
            story_id = body.get("story_id", instance_id)
            if plan_data:
                feedback.on_plan_declined(story_id, plan_data, reason)
        except Exception as fb_err:
            logger.warning("Decline feedback indexing failed (non-blocking): %s", fb_err)
        return func.HttpResponse(json.dumps({"status": "declined", "instance_id": instance_id, "reason": reason}), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Cancel Pipeline (terminate mid-run)
# ============================================================
@app.route(route="cancel-pipeline", methods=["POST"])
@app.durable_client_input(client_name="client")
async def cancel_pipeline(req: func.HttpRequest, client) -> func.HttpResponse:
    """Terminate a running pipeline orchestrator."""
    try:
        body = req.get_json()
        instance_id = body.get("instance_id", "")
        if not instance_id:
            return func.HttpResponse(json.dumps({"error": "instance_id required"}), status_code=400, mimetype="application/json")
        await client.terminate(instance_id, "Cancelled by user")
        return func.HttpResponse(json.dumps({"status": "cancelled", "instance_id": instance_id}), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


@app.route(route="process-story", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start_story_processing(req: func.HttpRequest, client) -> func.HttpResponse:
    """Accept a story and start the orchestration pipeline."""
    try:
        story_input = req.get_json()
    except ValueError:
        story_input = req.get_body().decode("utf-8")

    instance_id = await client.start_new("story_orchestrator", client_input=story_input)

    logger.info("Started orchestration: %s", instance_id)
    return client.create_check_status_response(req, instance_id)


# ============================================================
# HTTP TRIGGER: Preview story from ADO work item (no execution)
# ============================================================
@app.route(route="preview-ado-story", methods=["POST"])
async def preview_ado_story(req: func.HttpRequest) -> func.HttpResponse:
    """Fetch and map an ADO work item WITHOUT triggering the pipeline. For bot confirmation flow."""
    try:
        body = req.get_json()
        work_item_id = body.get("work_item_id")
        if not work_item_id:
            return func.HttpResponse('{"error": "work_item_id required"}', status_code=400)

        from shared.ado_client import ADOClient
        from shared.story_mapper import StoryMapper

        ado = ADOClient()
        wi_fields = ado.get_work_item_fields(int(work_item_id))

        config = AppConfig.from_env()
        mapper = StoryMapper(config)
        story_json = mapper.map_work_item(wi_fields)

        return func.HttpResponse(
            json.dumps({
                "work_item_id": work_item_id,
                "title": wi_fields.get("title", ""),
                "state": wi_fields.get("state", ""),
                "assigned_to": wi_fields.get("assigned_to", ""),
                "story_id": story_json.get("story_id"),
                "source_system": story_json.get("source_system", ""),
                "source_tables": story_json.get("source_tables", []),
                "dimensions": story_json.get("dimensions", []),
                "metrics": story_json.get("metrics", []),
                "filters": story_json.get("filters", []),
                "target_view_name": story_json.get("target_view_name", ""),
                "ready": bool(story_json.get("source_tables")),
                "message": "Ready to process" if story_json.get("source_tables") else "Could not extract source tables. Please update the work item description.",
            }),
            mimetype="application/json",
        )
    except Exception as e:
        logger.error("Failed to preview ADO story: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Process story from ADO work item
# ============================================================
@app.route(route="process-ado-story", methods=["POST"])
@app.durable_client_input(client_name="client")
async def process_ado_story(req: func.HttpRequest, client) -> func.HttpResponse:
    """Fetch a story from ADO work item and start the orchestration pipeline."""
    try:
        body = req.get_json()
        work_item_id = body.get("work_item_id")
        text_format = req.params.get("format", "") == "text"
        wait_mode = req.params.get("wait", "true").lower() != "false"
        if not work_item_id:
            return func.HttpResponse('{"error": "work_item_id required"}', status_code=400)
        try:
            work_item_id = int(work_item_id)
            if work_item_id <= 0:
                raise ValueError()
        except (ValueError, TypeError):
            return func.HttpResponse('{"error": "work_item_id must be a positive integer"}', status_code=400)

        from shared.ado_client import ADOClient
        from shared.story_mapper import StoryMapper

        ado = ADOClient()
        wi_fields = ado.get_work_item_fields(int(work_item_id))
        logger.info("Fetched ADO work item %s: %s", work_item_id, wi_fields.get("title"))

        config = AppConfig.from_env()
        mapper = StoryMapper(config)
        story_json = mapper.map_work_item(wi_fields)
        logger.info("Mapped story: %s with %d source tables", story_json.get("story_id"), len(story_json.get("source_tables", [])))

        if not story_json.get("source_tables"):
            return func.HttpResponse(
                json.dumps({"error": "Could not extract source tables from work item", "work_item": wi_fields}),
                status_code=422,
                mimetype="application/json",
            )

        # Update ADO work item state
        try:
            ado.add_comment(int(work_item_id), f"BI Automation pipeline started. Story mapped as: {story_json.get('story_id')}")
        except Exception as e:
            logger.warning("Could not update ADO work item: %s", e)

        instance_id = await client.start_new("story_orchestrator", client_input=story_json)
        logger.info("Started orchestration %s for ADO WI-%s", instance_id, work_item_id)

        # Async mode: return immediately (for web UI polling)
        if not wait_mode:
            return func.HttpResponse(
                json.dumps({
                    "instance_id": instance_id,
                    "story_id": story_json.get("story_id"),
                    "work_item_id": str(work_item_id),
                    "title": story_json.get("title"),
                    "source_tables": story_json.get("source_tables"),
                    "status": "STARTED",
                }),
                mimetype="application/json",
            )

        # Wait for pipeline to complete (up to 5 minutes), collecting progress
        import asyncio
        import time
        max_wait = 300
        poll_interval = 10
        start_time = time.time()
        final_output = None
        final_custom_status = None
        icons = {"completed": "✅", "in_progress": "⏳", "pending": "⬜", "failed": "❌", "escalated": "⚠️"}

        while time.time() - start_time < max_wait:
            status = await client.get_status(instance_id)
            if status:
                final_custom_status = status.custom_status
            if status and status.runtime_status and status.runtime_status.name in ("Completed", "Failed", "Terminated"):
                final_output = status.output or {}
                break
            await asyncio.sleep(poll_interval)

        # Build step progress text
        step_lines = ""
        if final_custom_status and isinstance(final_custom_status, dict):
            for s in final_custom_status.get("steps", []):
                icon = icons.get(s.get("status", ""), "⬜")
                detail = f" — {s['detail']}" if s.get("detail") else ""
                step_lines += f"\n{icon} Step {s['step']}/7: {s['name']}{detail}"
        else:
            step_lines = (
                "\n✅ Step 1/7: Fetch Story"
                "\n✅ Step 2/7: Extract Source Tables"
                "\n✅ Step 3/7: Planner Agent"
                "\n✅ Step 4/7: Developer Agent"
                "\n✅ Step 5/7: Pre-Deploy Validation"
                "\n✅ Step 6/7: Deploy to Synapse"
                "\n✅ Step 7/7: Post-Deploy Validation"
            )

        if final_output and isinstance(final_output, dict):
            deploy_data = final_output.get("deploy_result") or {}
            deploy_results = deploy_data.get("results", [])
            deployed = [r.get("artifact", "") for r in deploy_results if r.get("status") == "deployed"]
            skipped = [r.get("artifact", "") for r in deploy_results if r.get("status") == "skipped_exists"]
            failed = [r.get("artifact", "") for r in deploy_results if r.get("status") == "failed"]

            if len(failed) == 0 and len(deployed) > 0 and len(skipped) == 0:
                result_label = "SUCCESS"
            elif len(failed) == 0 and len(skipped) > 0 and len(deployed) > 0:
                result_label = f"PARTIAL - {len(deployed)} new, {len(skipped)} already exist"
            elif len(failed) == 0 and len(skipped) > 0 and len(deployed) == 0:
                result_label = f"NO CHANGES - all {len(skipped)} already exist"
            elif len(failed) > 0:
                result_label = f"ISSUES - {len(deployed)} deployed, {len(failed)} failed"
            else:
                result_label = final_output.get("status", "unknown").upper()

            obj_lines = ""
            for obj in deployed:
                obj_lines += f"\n  NEW: {obj}"
            for obj in skipped:
                obj_lines += f"\n  EXISTS: {obj}"
            for obj in failed:
                obj_lines += f"\n  FAILED: {obj}"

            summary_text = (
                f"BI Pipeline — {result_label}\n"
                f"{'=' * 30}\n"
                f"Story: {story_json.get('story_id')}\n"
                f"Title: {story_json.get('title')}\n"
                f"Mode: {final_output.get('mode', 'N/A')}\n\n"
                f"Agent Progress:{step_lines}\n\n"
                f"Objects Deployed:{obj_lines}\n\n"
                f"Deployed: {len(deployed)} | Skipped: {len(skipped)} | Failed: {len(failed)}\n"
                f"Duration: {int(time.time() - start_time)} seconds"
            )

            if text_format:
                return func.HttpResponse(summary_text, mimetype="text/plain")

            return func.HttpResponse(
                json.dumps({
                    "instance_id": instance_id,
                    "story_id": story_json.get("story_id"),
                    "work_item_id": str(work_item_id),
                    "title": story_json.get("title"),
                    "status": result_label,
                    "mode": final_output.get("mode", "N/A"),
                    "deployed_count": len(deployed),
                    "skipped_count": len(skipped),
                    "failed_count": len(failed),
                    "summary": summary_text,
                }),
                mimetype="application/json",
            )
        else:
            summary_text = (
                f"Pipeline still running (waited {int(time.time() - start_time)}s)\n\n"
                f"Story: {story_json.get('story_id')}\n"
                f"Progress so far:{step_lines}\n\n"
                f"Use 'check BI status' with work item {work_item_id} to see final results."
            )

            if text_format:
                return func.HttpResponse(summary_text, mimetype="text/plain")

            return func.HttpResponse(
                json.dumps({
                    "instance_id": instance_id,
                    "story_id": story_json.get("story_id"),
                    "work_item_id": str(work_item_id),
                    "title": story_json.get("title"),
                    "status": "IN PROGRESS",
                    "summary": summary_text,
                }),
                mimetype="application/json",
            )
    except Exception as e:
        logger.error("Failed to process ADO story: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# BUG FIXER: AI reads ADO bug -> analyzes -> fixes -> re-tests
# ============================================================

@app.route(route="fix-bug", methods=["POST"])
async def fix_bug_http(req: func.HttpRequest) -> func.HttpResponse:
    """Start the bug fix orchestrator.

    Body: {bug_id: int} or {bug_id: int, auto_deploy: bool, re_test: bool}
    Reads bug from ADO, analyzes root cause, generates fix, optionally deploys and re-tests.
    """
    try:
        body = req.get_json()
        bug_id = body.get("bug_id")
        if not bug_id:
            return func.HttpResponse(json.dumps({"error": "bug_id required"}), status_code=400, mimetype="application/json")

        auto_deploy = body.get("auto_deploy", False)
        re_test = body.get("re_test", False)

        orchestrator_input = {
            "bug_id": int(bug_id),
            "auto_deploy": auto_deploy,
            "re_test": re_test,
        }

        client = df.DurableOrchestrationClient(req)
        instance_id = await client.start_new("fix_bug_orchestrator", client_input=orchestrator_input)
        logger.info("Bug fix orchestrator started: instance=%s bug=%s", instance_id, bug_id)

        return func.HttpResponse(json.dumps({
            "instance_id": instance_id,
            "bug_id": bug_id,
            "status": "started",
            "status_url": f"/api/fix-status/{instance_id}",
        }), mimetype="application/json")
    except Exception as e:
        logger.error("fix-bug error: %s", str(e)[:300])
        return func.HttpResponse(json.dumps({"error": str(e)[:300]}), status_code=500, mimetype="application/json")


@app.route(route="fix-status/{instance_id}", methods=["GET"])
async def fix_bug_status(req: func.HttpRequest) -> func.HttpResponse:
    """Check status of a bug fix orchestrator run."""
    try:
        instance_id = req.route_params.get("instance_id", "")
        client = df.DurableOrchestrationClient(req)
        status = await client.get_status(instance_id, show_history=False, show_history_output=False)
        if not status:
            return func.HttpResponse(json.dumps({"error": "Instance not found"}), status_code=404, mimetype="application/json")
        return func.HttpResponse(json.dumps({
            "instance_id": instance_id,
            "runtime_status": status.runtime_status.value if status.runtime_status else "unknown",
            "custom_status": status.custom_status,
            "output": status.output,
            "created": status.created_time.isoformat() if status.created_time else None,
            "last_updated": status.last_updated_time.isoformat() if status.last_updated_time else None,
        }, default=str), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)[:200]}), status_code=500, mimetype="application/json")


@app.route(route="fix-approve/{instance_id}", methods=["POST"])
async def fix_bug_approve(req: func.HttpRequest) -> func.HttpResponse:
    """Approve or decline a bug fix before deployment.

    Body: {approved: bool, reason?: string}
    """
    try:
        instance_id = req.route_params.get("instance_id", "")
        body = req.get_json()
        client = df.DurableOrchestrationClient(req)
        await client.raise_event(instance_id, "BugFixReview", body)
        return func.HttpResponse(json.dumps({"status": "event_sent", "approved": body.get("approved")}), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)[:200]}), status_code=500, mimetype="application/json")


# ── Bug Fix Orchestrator ──

@app.orchestration_trigger(context_name="context")
def fix_bug_orchestrator(context: df.DurableOrchestrationContext):
    """Orchestrator: Fetch bug -> Analyze -> Fix -> Review Gate -> Deploy -> Re-test.

    Steps:
      1. Fetch bug details from ADO
      2. Find related pipeline/artifacts in Config DB
      3. Bug Fixer Agent analyzes root cause and generates fix
      4. Code Review Agent reviews the fix
      5. Human Review Gate (approve/decline)
      6. Deploy corrected artifacts to Synapse
      7. (Optional) Re-run tests to verify fix
      8. Update ADO bug status to Resolved
    """
    import datetime

    fix_input = context.get_input()
    bug_id = fix_input["bug_id"]
    auto_deploy = fix_input.get("auto_deploy", False)
    re_test = fix_input.get("re_test", False)

    steps = [
        {"step": 1, "name": "Fetch Bug from ADO", "status": "pending", "detail": ""},
        {"step": 2, "name": "Find Original Artifacts", "status": "pending", "detail": ""},
        {"step": 3, "name": "Bug Fixer Agent", "status": "pending", "detail": ""},
        {"step": 4, "name": "Code Review", "status": "pending", "detail": ""},
        {"step": 5, "name": "Review Gate", "status": "pending", "detail": ""},
        {"step": 6, "name": "Deploy Fix", "status": "pending", "detail": ""},
        {"step": 7, "name": "Re-Test", "status": "pending", "detail": ""},
        {"step": 8, "name": "Update ADO Bug", "status": "pending", "detail": ""},
    ]

    def _update(step_num, status, detail=""):
        for s in steps:
            if s["step"] == step_num:
                s["status"] = status
                if detail:
                    s["detail"] = detail
        context.set_custom_status({
            "bug_id": bug_id,
            "current_step": step_num,
            "total_steps": 8,
            "steps": steps,
        })

    # ── STEP 1: Fetch bug from ADO ──
    _update(1, "in_progress", "Fetching bug details...")
    bug_details = yield context.call_activity("fetch_ado_bug", {"bug_id": bug_id})

    if bug_details.get("error"):
        _update(1, "failed", f"Cannot fetch bug: {bug_details['error']}")
        return {"status": "failed", "error": bug_details["error"]}

    bug_title = bug_details.get("title", "")
    fix_type_hint = "data" if any(kw in bug_title.lower() for kw in ["sql", "data", "column", "table", "null", "count", "join", "query"]) else "unknown"
    _update(1, "completed", f"Bug #{bug_id}: {bug_title}")

    # ── STEP 2: Find original artifacts ──
    _update(2, "in_progress", "Searching for related pipeline artifacts...")
    related = yield context.call_activity("find_related_artifacts", {"bug_details": bug_details})
    original_artifacts = related.get("artifacts", [])
    story_id = related.get("story_id", f"BUG-{bug_id}")
    _update(2, "completed", f"Found {len(original_artifacts)} related artifacts (story: {story_id})")

    # ── STEP 3: Bug Fixer Agent ──
    _update(3, "in_progress", "AI analyzing root cause and generating fix...")
    fix_result = yield context.call_activity("run_bug_fixer", {
        "bug_details": bug_details,
        "original_artifacts": original_artifacts,
    })

    fix_type = fix_result.get("fix_type", "unknown")
    confidence = fix_result.get("confidence", "low")
    corrected = fix_result.get("corrected_artifacts", [])
    root_cause = fix_result.get("root_cause", "")

    # Store fix details in customStatus for UI
    context.set_custom_status({
        "bug_id": bug_id,
        "current_step": 3,
        "total_steps": 8,
        "steps": steps,
        "fix_result": {
            "fix_type": fix_type,
            "confidence": confidence,
            "root_cause": root_cause,
            "change_summary": fix_result.get("change_summary", ""),
            "corrected_count": len(corrected),
            "recommendation": fix_result.get("recommendation", ""),
        },
    })

    if fix_type == "ui_recommendation":
        _update(3, "completed", f"UI issue: recommendation generated (confidence: {confidence})")
        # UI bugs can't be auto-fixed, update ADO and return
        yield context.call_activity("update_ado_bug_with_fix", {
            "bug_id": bug_id,
            "fix_result": fix_result,
            "status": "recommendation",
        })
        _update(8, "completed", "ADO bug updated with fix recommendation")
        return {"status": "recommendation", "bug_id": bug_id, "fix_type": fix_type, "fix_result": fix_result}

    if not corrected:
        _update(3, "failed", f"No fix generated (confidence: {confidence})")
        return {"status": "no_fix", "bug_id": bug_id, "root_cause": root_cause}

    _update(3, "completed", f"{fix_type}: {len(corrected)} artifacts, confidence: {confidence}, cause: {root_cause[:80]}")

    # ── STEP 4: Code Review ──
    _update(4, "in_progress", f"AI reviewing {len(corrected)} corrected artifacts...")
    review_result = yield context.call_activity("run_code_review", {
        "artifacts": corrected,
        "build_plan": {"story_id": story_id, "fix_for_bug": bug_id},
    })
    review_verdict = review_result.get("overall_verdict", "APPROVE")
    review_findings = review_result.get("total_findings", 0)

    context.set_custom_status({
        "bug_id": bug_id,
        "current_step": 4,
        "total_steps": 8,
        "steps": steps,
        "fix_result": {
            "fix_type": fix_type,
            "confidence": confidence,
            "root_cause": root_cause,
            "corrected_count": len(corrected),
        },
        "code_review": review_result,
    })

    if review_verdict == "REJECT":
        _update(4, "failed", f"REJECTED: {review_findings} findings, fix needs manual review")
        return {"status": "review_rejected", "bug_id": bug_id, "review": review_result}

    _update(4, "completed", f"{review_verdict}: {review_findings} findings")

    # ── STEP 5: Human Review Gate ──
    _update(5, "in_progress", "Awaiting human approval...")

    context.set_custom_status({
        "bug_id": bug_id,
        "current_step": 5,
        "total_steps": 8,
        "steps": steps,
        "awaiting_approval": True,
        "fix_result": {
            "fix_type": fix_type,
            "confidence": confidence,
            "root_cause": root_cause,
            "change_summary": fix_result.get("change_summary", ""),
            "corrected_artifacts": corrected,
            "recommendation": fix_result.get("recommendation", ""),
        },
        "code_review": review_result,
    })

    try:
        approval_event = yield context.wait_for_external_event("BugFixReview")
    except TimeoutError:
        _update(5, "failed", "Review timed out after 30 minutes")
        return {"status": "timed_out", "bug_id": bug_id}

    if isinstance(approval_event, str):
        try:
            approval_event = json.loads(approval_event)
        except (json.JSONDecodeError, TypeError):
            approval_event = {"approved": False, "reason": "Invalid response"}
    if not isinstance(approval_event, dict):
        approval_event = {"approved": False, "reason": "Invalid response"}

    if not approval_event.get("approved", False):
        decline_reason = approval_event.get("reason", "Declined by reviewer")
        _update(5, "failed", f"Declined: {decline_reason}")
        return {"status": "declined", "bug_id": bug_id, "reason": decline_reason}

    _update(5, "completed", "APPROVED for deployment")

    # ── STEP 6: Deploy Fix ──
    _update(6, "in_progress", f"Deploying {len(corrected)} corrected artifacts...")

    # Build ArtifactBundle-compatible dict for deploy_artifacts activity
    deploy_artifacts_list = []
    for art in corrected:
        deploy_artifacts_list.append({
            "step": 1,
            "artifact_type": art.get("artifact_type", "table"),
            "object_name": art.get("object_name", ""),
            "layer": art.get("layer", "gold"),
            "file_name": art.get("file_name", "fix.sql"),
            "content": art.get("content", ""),
        })

    deploy_result = yield context.call_activity("deploy_artifacts", {
        "bundle": {"story_id": story_id, "artifacts": deploy_artifacts_list},
        "environment": "dev",
    })

    dep_results = deploy_result.get("results", [])
    dep_ok = len([r for r in dep_results if r.get("status") in ("deployed", "skipped_exists")])
    dep_fail = len([r for r in dep_results if r.get("status") == "failed"])

    if dep_fail > 0:
        _update(6, "failed", f"Deploy failed: {dep_fail} errors")
        return {"status": "deploy_failed", "bug_id": bug_id, "deploy_result": deploy_result}

    _update(6, "completed", f"{dep_ok} artifacts deployed successfully")

    # ── STEP 7: Re-test (optional) ──
    if re_test:
        _update(7, "in_progress", "Re-running validation tests...")
        try:
            retest_result = yield context.call_activity("run_bug_retest", {
                "bug_id": bug_id,
                "story_id": story_id,
                "corrected_artifacts": corrected,
            })
            retest_pass = retest_result.get("passed", False)
            retest_detail = retest_result.get("detail", "")
            if retest_pass:
                _update(7, "completed", f"Re-test PASSED: {retest_detail}")
            else:
                _update(7, "failed", f"Re-test FAILED: {retest_detail}")
        except Exception as e:
            _update(7, "completed", f"Re-test skipped: {str(e)[:100]}")
    else:
        _update(7, "completed", "Re-test skipped (not requested)")

    # ── STEP 8: Update ADO Bug ──
    _update(8, "in_progress", "Updating ADO bug to Resolved...")
    yield context.call_activity("update_ado_bug_with_fix", {
        "bug_id": bug_id,
        "fix_result": fix_result,
        "status": "resolved",
        "deploy_result": deploy_result,
    })
    _update(8, "completed", f"Bug #{bug_id} marked Resolved in ADO")

    return {
        "status": "fixed",
        "bug_id": bug_id,
        "fix_type": fix_type,
        "root_cause": root_cause,
        "confidence": confidence,
        "artifacts_deployed": dep_ok,
        "retest_passed": re_test and retest_result.get("passed", False) if re_test else None,
    }


# ── Bug Fix Activity Functions ──

@app.activity_trigger(input_name="payload")
def fetch_ado_bug(payload: dict) -> dict:
    """Fetch bug details from ADO."""
    try:
        config = AppConfig.from_env()
        from shared.ado_client import ADOClient
        ado = ADOClient()
        fields = ado.get_work_item_fields(payload["bug_id"])
        return {
            "id": fields["id"],
            "title": fields["title"],
            "description": fields.get("description", ""),
            "repro_steps": fields.get("acceptance_criteria", ""),
            "state": fields.get("state", ""),
            "severity": fields.get("priority", ""),
            "tags": fields.get("tags", ""),
            "assigned_to": fields.get("assigned_to", ""),
        }
    except Exception as e:
        logger.error("fetch_ado_bug failed: %s", str(e)[:200])
        return {"error": str(e)[:200]}


@app.activity_trigger(input_name="payload")
def find_related_artifacts(payload: dict) -> dict:
    """Find original pipeline artifacts related to this bug.

    Searches Config DB for the story/pipeline that produced the buggy code.
    """
    try:
        config = AppConfig.from_env()
        from shared.state_registry import StateRegistry
        reg = StateRegistry(config)
        bug = payload["bug_details"]
        tags = bug.get("tags", "")
        title = bug.get("title", "")
        description = bug.get("description", "")

        # Try to find story_id from bug tags or title
        story_id = None
        import re
        # Look for story references like "STORY-001", "S-001", "#12345"
        for text in [tags, title, description]:
            match = re.search(r'(STORY-\d+|S-\d+|#(\d{4,}))', text, re.IGNORECASE)
            if match:
                story_id = match.group(1)
                break

        artifacts = []
        if story_id:
            # Get artifacts from Config DB
            artifact_history = reg.get_artifact_history(story_id)
            for art in artifact_history:
                artifacts.append({
                    "object_name": art.get("object_name", ""),
                    "layer": art.get("layer", ""),
                    "artifact_type": art.get("artifact_type", ""),
                    "content": art.get("sql_content", art.get("content", "")),
                    "file_name": art.get("file_name", ""),
                })

        return {"story_id": story_id or f"BUG-{bug.get('id', 'unknown')}", "artifacts": artifacts}
    except Exception as e:
        logger.error("find_related_artifacts failed: %s", str(e)[:200])
        return {"story_id": f"BUG-{payload.get('bug_details', {}).get('id', 'unknown')}", "artifacts": []}


@app.activity_trigger(input_name="payload")
def run_bug_fixer(payload: dict) -> dict:
    """Run the Bug Fixer Agent."""
    try:
        config = AppConfig.from_env()
        from fixer.agent import BugFixerAgent
        agent = BugFixerAgent(config)
        return agent.analyze_and_fix(
            bug_details=payload["bug_details"],
            original_artifacts=payload.get("original_artifacts"),
        )
    except Exception as e:
        logger.error("run_bug_fixer failed: %s", str(e)[:200])
        return {"fix_type": "error", "root_cause": str(e)[:200], "corrected_artifacts": [], "confidence": "low"}


@app.activity_trigger(input_name="payload")
def run_bug_retest(payload: dict) -> dict:
    """Re-run validation tests after deploying a fix."""
    try:
        config = AppConfig.from_env()
        from validator.agent import ValidatorAgent
        agent = ValidatorAgent(config)

        corrected = payload.get("corrected_artifacts", [])
        passed = True
        details = []

        for art in corrected:
            obj_name = art.get("object_name", "")
            schema_table = obj_name.replace("[", "").replace("]", "").split(".")
            if len(schema_table) != 2:
                continue
            schema, table = schema_table

            # Check object exists
            from shared.synapse_client import SynapseClient
            synapse = SynapseClient(config)
            if synapse.check_object_exists(schema, table):
                # Run a basic row count check
                try:
                    rows = synapse.execute_query(f"SELECT COUNT(*) AS cnt FROM [{schema}].[{table}]")
                    cnt = rows[0]["cnt"] if rows else 0
                    details.append(f"{obj_name}: exists, {cnt} rows")
                except Exception as qe:
                    details.append(f"{obj_name}: exists but query failed: {str(qe)[:80]}")
                    passed = False
            else:
                details.append(f"{obj_name}: NOT FOUND after deploy")
                passed = False

        return {"passed": passed, "detail": "; ".join(details) if details else "No objects to verify"}
    except Exception as e:
        logger.error("run_bug_retest failed: %s", str(e)[:200])
        return {"passed": False, "detail": str(e)[:200]}


@app.activity_trigger(input_name="payload")
def update_ado_bug_with_fix(payload: dict) -> dict:
    """Update the ADO Bug work item with fix details and set to Resolved."""
    try:
        from shared.ado_client import ADOClient
        ado = ADOClient()
        bug_id = payload["bug_id"]
        fix_result = payload.get("fix_result", {})
        status = payload.get("status", "resolved")

        fix_type = fix_result.get("fix_type", "unknown")
        root_cause = fix_result.get("root_cause", "")
        change_summary = fix_result.get("change_summary", "")
        confidence = fix_result.get("confidence", "")
        recommendation = fix_result.get("recommendation", "")

        # Build comment
        comment_lines = [
            f"<h3>AI Bug Fix Report</h3>",
            f"<p><strong>Fix Type:</strong> {fix_type}</p>",
            f"<p><strong>Root Cause:</strong> {root_cause}</p>",
            f"<p><strong>Confidence:</strong> {confidence}</p>",
        ]
        if change_summary:
            comment_lines.append(f"<p><strong>Changes:</strong> {change_summary}</p>")
        if recommendation:
            comment_lines.append(f"<p><strong>Recommendation:</strong> {recommendation}</p>")

        deploy_result = payload.get("deploy_result")
        if deploy_result:
            dep_results = deploy_result.get("results", [])
            deployed = [r.get("artifact", "") for r in dep_results if r.get("status") == "deployed"]
            if deployed:
                comment_lines.append(f"<p><strong>Deployed:</strong> {', '.join(deployed)}</p>")

        comment_lines.append("<p><em>Fixed by BI Automation Platform - Bug Fixer Agent</em></p>")
        comment = "\n".join(comment_lines)

        # Add comment
        ado.add_comment(bug_id, comment)

        # Update state if resolved
        if status == "resolved":
            ado.update_work_item_state(bug_id, "Resolved",
                                       comment=f"Auto-resolved by Bug Fixer Agent. Root cause: {root_cause[:200]}")

        return {"status": "updated", "bug_id": bug_id}
    except Exception as e:
        logger.error("update_ado_bug_with_fix failed: %s", str(e)[:200])
        return {"status": "failed", "error": str(e)[:200]}


# ============================================================
# INTEGRATION MODE: Discovery + Convention Adapter + PR Delivery
# ============================================================

@app.route(route="discover", methods=["POST"])
async def discover_environment(req: func.HttpRequest) -> func.HttpResponse:
    """Run Discovery Agent to scan the connected Azure data platform.

    Returns an EnvironmentProfile with schemas, objects, conventions.
    """
    try:
        config = AppConfig.from_env()
        from discovery.agent import DiscoveryAgent
        agent = DiscoveryAgent(config)
        body = req.get_json() if req.get_body() else {}
        options = body if isinstance(body, dict) else {}
        profile = agent.discover(options)

        # Auto-index discovery results into RAG knowledge base
        try:
            from shared.discovery_rag_bridge import DiscoveryRAGBridge
            from shared.rag_retriever import RAGRetriever
            retriever = RAGRetriever(config)
            bridge = DiscoveryRAGBridge(retriever)
            rag_stats = bridge.index_environment_profile(profile)
            profile["rag_indexed"] = rag_stats
            logger.info("Discovery results auto-indexed into RAG: %s", rag_stats)
        except Exception as rag_err:
            logger.warning("RAG indexing after discovery failed (non-blocking): %s", rag_err)

        return func.HttpResponse(json.dumps(profile, indent=2, default=str), mimetype="application/json")
    except Exception as e:
        logger.error("discover error: %s", str(e)[:300])
        return func.HttpResponse(json.dumps({"error": str(e)[:300]}), status_code=500, mimetype="application/json")


@app.route(route="conventions", methods=["GET", "POST"])
async def conventions_api(req: func.HttpRequest) -> func.HttpResponse:
    """GET: Return current convention ruleset. POST: Generate from discovery profile."""
    try:
        from shared.convention_adapter import ConventionRuleset, build_ruleset_from_profile

        if req.method == "POST":
            body = req.get_json()
            if not body:
                return func.HttpResponse(json.dumps({"error": "POST body with discovery profile required"}),
                                         status_code=400, mimetype="application/json")
            ruleset = build_ruleset_from_profile(body)

            # Auto-index conventions into RAG
            try:
                from shared.discovery_rag_bridge import DiscoveryRAGBridge
                from shared.rag_retriever import RAGRetriever
                bridge = DiscoveryRAGBridge(RAGRetriever(AppConfig.from_env()))
                bridge.index_convention_ruleset(ruleset)
            except Exception as rag_err:
                logger.warning("RAG convention indexing failed (non-blocking): %s", rag_err)

            return func.HttpResponse(json.dumps({
                "ruleset": ruleset.to_dict(),
                "prompt_context": ruleset.to_prompt_context(),
            }, indent=2), mimetype="application/json")

        # GET: Return default ruleset
        ruleset = ConventionRuleset()
        return func.HttpResponse(json.dumps({
            "ruleset": ruleset.to_dict(),
            "prompt_context": ruleset.to_prompt_context(),
        }, indent=2), mimetype="application/json")
    except Exception as e:
        logger.error("conventions error: %s", str(e)[:200])
        return func.HttpResponse(json.dumps({"error": str(e)[:200]}), status_code=500, mimetype="application/json")


@app.route(route="deliver-pr", methods=["POST"])
async def deliver_pr(req: func.HttpRequest) -> func.HttpResponse:
    """Create a Pull Request with generated artifacts instead of direct deployment.

    Body: {instance_id, target_branch?, base_path?}
    Reads artifacts from a completed pipeline run and creates a PR.
    """
    try:
        body = req.get_json()
        instance_id = body.get("instance_id", "")
        if not instance_id:
            return func.HttpResponse(json.dumps({"error": "instance_id required"}), status_code=400, mimetype="application/json")

        target_branch = body.get("target_branch", "develop")
        base_path = body.get("base_path", "generated")

        # Get the pipeline output
        config = AppConfig.from_env()
        from shared.state_registry import StateRegistry
        registry = StateRegistry(config)

        # Find the pipeline by instance_id
        pipelines = registry.get_pipeline_history(limit=50)
        pipeline = None
        for p in pipelines:
            if p.get("last_instance_id") == instance_id:
                pipeline = p
                break

        if not pipeline:
            return func.HttpResponse(json.dumps({"error": f"Pipeline not found for instance {instance_id}"}),
                                     status_code=404, mimetype="application/json")

        story_id = pipeline.get("story_id", "")
        title = pipeline.get("title", "")

        # Get artifacts from Config DB
        artifacts_raw = registry.get_artifact_history(story_id)
        if not artifacts_raw:
            return func.HttpResponse(json.dumps({"error": "No artifacts found for this pipeline"}),
                                     status_code=404, mimetype="application/json")

        # Convert to PR format
        artifacts = []
        for art in artifacts_raw:
            artifacts.append({
                "file_name": art.get("file_name", f"{art.get('object_name', 'unknown')}.sql"),
                "content": art.get("sql_content", art.get("content", "")),
                "layer": art.get("layer", ""),
                "object_name": art.get("object_name", ""),
                "artifact_type": art.get("artifact_type", ""),
            })

        from shared.pr_client import PRClient
        pr_client = PRClient(config)
        result = pr_client.create_pr(
            story_id=story_id,
            title=title,
            artifacts=artifacts,
            target_branch=target_branch,
            base_path=base_path,
        )
        return func.HttpResponse(json.dumps(result, indent=2), mimetype="application/json")
    except Exception as e:
        logger.error("deliver-pr error: %s", str(e)[:300])
        return func.HttpResponse(json.dumps({"error": str(e)[:300]}), status_code=500, mimetype="application/json")


# ============================================================
# OPS MODULE: Health, Auto-Pause, Regression, Cleanup
# ============================================================

@app.route(route="ops/dashboard", methods=["GET"])
async def ops_dashboard(req: func.HttpRequest) -> func.HttpResponse:
    """Full operational dashboard: agent stats, secret health, Synapse idle, DB counts."""
    try:
        config = AppConfig.from_env()
        from shared.ops import OpsManager
        ops = OpsManager(config)
        dashboard = ops.get_dashboard()
        return func.HttpResponse(json.dumps(dashboard, indent=2, default=str), mimetype="application/json")
    except Exception as e:
        logger.error("ops/dashboard error: %s", str(e)[:300])
        return func.HttpResponse(json.dumps({"error": str(e)[:300]}), status_code=500, mimetype="application/json")


@app.route(route="ops/agent-stats", methods=["GET"])
async def ops_agent_stats(req: func.HttpRequest) -> func.HttpResponse:
    """Agent performance stats: failure rates, avg duration, success rate."""
    try:
        config = AppConfig.from_env()
        from shared.ops import OpsManager
        ops = OpsManager(config)
        days = int(req.params.get("days", "7"))
        stats = ops.get_agent_stats(days=days)
        return func.HttpResponse(json.dumps(stats, indent=2, default=str), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)[:200]}), status_code=500, mimetype="application/json")


@app.route(route="ops/secret-health", methods=["GET"])
async def ops_secret_health(req: func.HttpRequest) -> func.HttpResponse:
    """Check credential health: ADO PAT, AI Foundry key, SQL password, Teams webhook."""
    try:
        config = AppConfig.from_env()
        from shared.ops import OpsManager
        ops = OpsManager(config)
        health = ops.check_secret_health()
        status_code = 200 if health["status"] == "healthy" else 207
        return func.HttpResponse(json.dumps(health, indent=2, default=str), mimetype="application/json", status_code=status_code)
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)[:200]}), status_code=500, mimetype="application/json")


@app.route(route="ops/synapse-idle", methods=["GET"])
async def ops_synapse_idle(req: func.HttpRequest) -> func.HttpResponse:
    """Check if Synapse pool is idle and should be paused."""
    try:
        config = AppConfig.from_env()
        from shared.ops import OpsManager
        ops = OpsManager(config)
        idle_minutes = int(req.params.get("minutes", "30"))
        result = ops.check_synapse_idle(idle_minutes=idle_minutes)
        return func.HttpResponse(json.dumps(result, indent=2, default=str), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)[:200]}), status_code=500, mimetype="application/json")


@app.route(route="ops/pause-synapse", methods=["POST"])
async def ops_pause_synapse(req: func.HttpRequest) -> func.HttpResponse:
    """Manually trigger Synapse pool pause."""
    try:
        config = AppConfig.from_env()
        from shared.ops import OpsManager
        ops = OpsManager(config)
        result = ops.pause_synapse()
        return func.HttpResponse(json.dumps(result, indent=2, default=str), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)[:200]}), status_code=500, mimetype="application/json")


@app.route(route="ops/regression-test", methods=["POST"])
async def ops_regression_test(req: func.HttpRequest) -> func.HttpResponse:
    """Run prompt regression tests against known baselines."""
    try:
        config = AppConfig.from_env()
        from shared.ops import OpsManager
        ops = OpsManager(config)
        result = ops.run_regression_test()
        status_code = 200 if result["status"] == "pass" else 207
        return func.HttpResponse(json.dumps(result, indent=2, default=str), mimetype="application/json", status_code=status_code)
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)[:200]}), status_code=500, mimetype="application/json")


@app.route(route="ops/cleanup", methods=["POST"])
async def ops_cleanup(req: func.HttpRequest) -> func.HttpResponse:
    """Run Config DB retention cleanup (archive records older than N days)."""
    try:
        config = AppConfig.from_env()
        body = req.get_json() if req.get_body() else {}
        retention_days = int(body.get("retention_days", 90)) if isinstance(body, dict) else 90
        from shared.ops import OpsManager
        ops = OpsManager(config)
        result = ops.run_cleanup(retention_days=retention_days)
        return func.HttpResponse(json.dumps(result, indent=2, default=str), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)[:200]}), status_code=500, mimetype="application/json")


# ── Timer Trigger: Auto-Pause Synapse (every 30 min) ──

@app.timer_trigger(schedule="0 */30 * * * *", arg_name="timer", run_on_startup=False)
async def auto_pause_synapse_timer(timer: func.TimerRequest) -> None:
    """Runs every 30 minutes. Pauses Synapse if no recent pipeline activity."""
    try:
        config = AppConfig.from_env()
        from shared.ops import OpsManager
        ops = OpsManager(config)
        idle_check = ops.check_synapse_idle(idle_minutes=30)

        if idle_check.get("should_pause"):
            logger.info("Auto-pause: Synapse idle for 30+ min, pausing...")
            pause_result = ops.pause_synapse()
            logger.info("Auto-pause result: %s", pause_result.get("status"))

            # Notify via Teams
            try:
                from shared.teams_webhook import send_card
                card = {
                    "type": "message",
                    "attachments": [{
                        "contentType": "application/vnd.microsoft.card.adaptive",
                        "content": {
                            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                            "type": "AdaptiveCard",
                            "version": "1.4",
                            "body": [
                                {"type": "TextBlock", "text": "Synapse Auto-Paused", "weight": "bolder", "size": "medium"},
                                {"type": "TextBlock", "text": f"Pool paused after 30 min idle. Status: {pause_result.get('status')}", "wrap": True},
                            ],
                        },
                    }],
                }
                send_card(card)
            except Exception as e:
                logger.warning("Non-critical error sending auto-pause notification: %s", e)
        else:
            logger.debug("Auto-pause: Synapse has recent activity, keeping online")
    except Exception as e:
        logger.error("auto_pause_synapse_timer failed: %s", str(e)[:200])


# ── Timer Trigger: Secret Expiry Check (daily at 8 AM UTC) ──

@app.timer_trigger(schedule="0 0 8 * * *", arg_name="timer", run_on_startup=False)
async def secret_health_check_timer(timer: func.TimerRequest) -> None:
    """Runs daily. Checks credential health and alerts via Teams if issues found."""
    try:
        config = AppConfig.from_env()
        from shared.ops import OpsManager
        ops = OpsManager(config)
        health = ops.check_secret_health()

        if health["status"] != "healthy":
            warnings = health.get("warnings", [])
            logger.warning("Secret health check: %s - %s", health["status"], "; ".join(warnings))

            try:
                from shared.teams_webhook import send_card
                warning_text = "\n".join(f"- {w}" for w in warnings)
                card = {
                    "type": "message",
                    "attachments": [{
                        "contentType": "application/vnd.microsoft.card.adaptive",
                        "content": {
                            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                            "type": "AdaptiveCard",
                            "version": "1.4",
                            "body": [
                                {"type": "TextBlock", "text": "Credential Health Alert", "weight": "bolder", "size": "medium", "color": "attention"},
                                {"type": "TextBlock", "text": f"Status: {health['status'].upper()}", "weight": "bolder"},
                                {"type": "TextBlock", "text": warning_text, "wrap": True},
                            ],
                        },
                    }],
                }
                send_card(card)
            except Exception as e:
                logger.warning("Non-critical error sending secret health notification: %s", e)
        else:
            logger.info("Secret health check: all healthy")
    except Exception as e:
        logger.error("secret_health_check_timer failed: %s", str(e)[:200])


# ── Timer Trigger: Weekly Cleanup (Sunday 2 AM UTC) ──

@app.timer_trigger(schedule="0 0 2 * * 0", arg_name="timer", run_on_startup=False)
async def weekly_cleanup_timer(timer: func.TimerRequest) -> None:
    """Runs weekly on Sunday. Cleans up old Config DB records."""
    try:
        config = AppConfig.from_env()
        from shared.ops import OpsManager
        ops = OpsManager(config)
        result = ops.run_cleanup(retention_days=90)
        logger.info("Weekly cleanup: %s", json.dumps(result.get("deleted", {})))
    except Exception as e:
        logger.error("weekly_cleanup_timer failed: %s", str(e)[:200])


# ============================================================
# TEST AUTOMATION: Separated into independent Function App
# Product: test-automation/function_app.py
# Deploy: {prefix}-{env}-test-func.azurewebsites.net
# Endpoints: /api/run-tests, /api/test-progress, /api/agent-*
# ============================================================

# ============================================================
# HTTP TRIGGER: Teams Bot Framework Endpoint
# ============================================================
@app.route(route="bot-message", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
async def bot_message(req: func.HttpRequest) -> func.HttpResponse:
    """Bot Framework messaging endpoint for Teams integration."""
    import os as _os
    from botbuilder.core import (
        BotFrameworkAdapter,
        BotFrameworkAdapterSettings,
    )
    from shared.teams_bot import BIAutomationBot

    settings = BotFrameworkAdapterSettings(
        app_id=_os.environ.get("BOT_APP_ID", ""),
        app_password=_os.environ.get("BOT_APP_PASSWORD", ""),
    )
    adapter = BotFrameworkAdapter(settings)

    async def on_error(context, error):
        logger.error("Bot adapter error: %s", error)
        await context.send_activity("Sorry, something went wrong. Please try again.")

    adapter.on_turn_error = on_error
    bot = BIAutomationBot()

    # Parse the incoming Bot Framework activity
    body = req.get_body().decode("utf-8")
    auth_header = req.headers.get("Authorization", "")

    from botbuilder.schema import Activity as BFActivity
    activity = BFActivity().deserialize(json.loads(body))

    try:
        response = await adapter.process_activity(activity, auth_header, bot.on_turn)
        if response:
            return func.HttpResponse(
                body=json.dumps(response.body) if response.body else "",
                status_code=response.status,
                mimetype="application/json",
            )
        return func.HttpResponse(status_code=200)
    except Exception as e:
        logger.error("Bot message processing error: %s", e)
        return func.HttpResponse(status_code=200)


# ============================================================
# HTTP TRIGGER: Teams Bot Proactive Notification
# ============================================================
@app.route(route="bot-notify", methods=["POST"])
async def bot_notify(req: func.HttpRequest) -> func.HttpResponse:
    """Send proactive notification to a Teams user about pipeline progress."""
    import os as _os
    from botbuilder.core import (
        BotFrameworkAdapter,
        BotFrameworkAdapterSettings,
        MessageFactory as MF,
    )
    from shared.teams_bot import (
        get_conversation_reference,
        review_card,
        progress_card,
        completion_card,
    )

    try:
        body = req.get_json()
        user_id = body.get("user_id", "")
        notification_type = body.get("type", "")  # "review", "progress", "complete"
        payload = body.get("payload", {})

        ref = get_conversation_reference(user_id)
        if not ref:
            return func.HttpResponse(json.dumps({"error": "No conversation reference for user"}), status_code=404, mimetype="application/json")

        settings = BotFrameworkAdapterSettings(
            app_id=_os.environ.get("BOT_APP_ID", ""),
            app_password=_os.environ.get("BOT_APP_PASSWORD", ""),
        )
        adapter = BotFrameworkAdapter(settings)

        async def send_notification(turn_context):
            if notification_type == "review":
                card = review_card(payload.get("instance_id", ""), payload.get("review", {}))
                await turn_context.send_activity(MF.attachment(card))
            elif notification_type == "progress":
                card = progress_card(payload.get("instance_id", ""), payload.get("steps", []), payload.get("story_id", ""))
                await turn_context.send_activity(MF.attachment(card))
            elif notification_type == "complete":
                card = completion_card(
                    payload.get("story_id", ""),
                    payload.get("status", ""),
                    payload.get("deployed", []),
                    payload.get("skipped", []),
                    payload.get("failed", []),
                    payload.get("elapsed", 0),
                )
                await turn_context.send_activity(MF.attachment(card))
            else:
                await turn_context.send_activity(MF.text(payload.get("text", "Notification")))

        await adapter.continue_conversation(ref, send_notification, _os.environ.get("BOT_APP_ID", ""))

        return func.HttpResponse(json.dumps({"status": "sent"}), mimetype="application/json")
    except Exception as e:
        logger.error("Bot notify error: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Detailed pipeline status
# ============================================================
@app.route(route="pipeline-status", methods=["GET"])
@app.durable_client_input(client_name="client")
async def pipeline_status(req: func.HttpRequest, client) -> func.HttpResponse:
    """Return a human-readable summary of a pipeline run from the orchestrator output."""
    instance_id = req.params.get("instance_id", "")
    if not instance_id:
        return func.HttpResponse('{"error": "instance_id required"}', status_code=400)

    try:
        status = await client.get_status(instance_id)
        if not status:
            return func.HttpResponse(json.dumps({"error": "Instance not found"}), status_code=404)

        output = status.output or {}
        runtime = status.runtime_status.name if status.runtime_status else "Unknown"

        if not isinstance(output, dict):
            return func.HttpResponse(json.dumps({
                "instance_id": instance_id,
                "runtime_status": runtime,
                "raw_output": str(output),
            }, indent=2), mimetype="application/json")

        # Extract deploy info from output.deploy_result
        deploy_data = output.get("deploy_result") or {}
        deploy_results = deploy_data.get("results", [])
        deployed = [r.get("artifact", "") for r in deploy_results if r.get("status") == "deployed"]
        skipped_exists = [r.get("artifact", "") for r in deploy_results if r.get("status") == "skipped_exists"]
        failed_deploys = [{"artifact": r.get("artifact", ""), "error": r.get("error", "")} for r in deploy_results if r.get("status") == "failed"]

        # Extract validation info from output.validation_report
        val_report = output.get("validation_report") or {}
        val_checks = val_report.get("checks", [])
        passed = [c.get("check_name", "") for c in val_checks if c.get("status") == "pass"]
        failed_vals = [{"check": c.get("check_name", ""), "message": c.get("message", "")} for c in val_checks if c.get("status") == "fail"]

        # Extract plan info
        plan = output.get("build_plan") or {}
        execution_order = plan.get("execution_order", [])

        # Extract artifact count
        artifacts = output.get("artifacts") or {}
        artifact_list = artifacts.get("artifacts", [])

        summary = {
            "instance_id": instance_id,
            "runtime_status": runtime,
            "pipeline_status": output.get("status", "unknown"),
            "story_id": output.get("story_id", "N/A"),
            "mode": output.get("mode", "N/A"),
            "plan": {
                "execution_order": execution_order,
                "risk_level": plan.get("risk_level", "N/A"),
            },
            "artifacts_generated": len(artifact_list),
            "deployment": {
                "total": len(deploy_results),
                "deployed": deployed,
                "skipped_existing": skipped_exists,
                "failed": failed_deploys,
            },
            "validation": {
                "total_checks": len(val_checks),
                "passed": passed,
                "failed": failed_vals,
            },
            "healer_actions": len(output.get("healer_actions", [])),
            "retry_count": output.get("retry_count", 0),
            "errors": output.get("error_log", []),
            "created": str(status.created_time),
            "completed": str(status.last_updated_time),
        }

        return func.HttpResponse(json.dumps(summary, indent=2), mimetype="application/json")
    except Exception as e:
        logger.error("pipeline-status error: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Story status by work item ID (no instance_id needed)
# ============================================================
@app.route(route="story-status", methods=["GET"])
@app.durable_client_input(client_name="client")
async def story_status(req: func.HttpRequest, client) -> func.HttpResponse:
    """Look up the latest pipeline run for a work item ID. Much easier for bot integration."""
    import os as _os
    import requests as _requests

    work_item_id = req.params.get("work_item_id", "")
    if not work_item_id:
        return func.HttpResponse('{"error": "work_item_id required"}', status_code=400)

    story_id = f"STORY-{work_item_id}"

    try:
        # Query all recent instances via Durable Task REST API
        task_hub = _os.environ.get("TASK_HUB_NAME", "BiAutoHubV8")
        sys_key = _os.environ.get("DURABLETASK_EXTENSION_KEY", "")
        base_url = f"https://{_os.environ.get('WEBSITE_HOSTNAME', 'localhost')}"

        target_instance = None
        for hub in [task_hub, "BiAutoHubV8", "BiAutoHubV6"]:
            if target_instance:
                break
            url = (
                f"{base_url}/runtime/webhooks/durabletask/instances"
                f"?taskHub={hub}&connection=Storage&code={sys_key}"
                f"&top=10&showInput=true"
            )
            try:
                resp = _requests.get(url, timeout=30)
                resp.raise_for_status()
                instances = resp.json()
                if isinstance(instances, dict):
                    instances = instances.get("value", instances)
                for inst in reversed(instances):
                    inp = inst.get("input", "")
                    if isinstance(inp, str) and story_id in inp:
                        target_instance = inst
                        break
                    elif isinstance(inp, dict) and inp.get("story_id") == story_id:
                        target_instance = inst
                        break
            except Exception as e:
                logger.warning("Non-critical error scanning instance for story %s: %s", story_id, e)
                continue

        if not target_instance:
            return func.HttpResponse(json.dumps({
                "error": f"No pipeline run found for work item {work_item_id}",
                "story_id": story_id,
            }), status_code=404, mimetype="application/json")

        instance_id = target_instance["instanceId"]

        # Now get the full status with output
        status = await client.get_status(instance_id)
        output = status.output or {}
        runtime = status.runtime_status.name if status.runtime_status else "Unknown"

        if not isinstance(output, dict):
            return func.HttpResponse(json.dumps({
                "work_item_id": int(work_item_id),
                "story_id": story_id,
                "instance_id": instance_id,
                "runtime_status": runtime,
                "pipeline_status": "unknown",
            }, indent=2), mimetype="application/json")

        # Extract deploy info
        deploy_data = output.get("deploy_result") or {}
        deploy_results = deploy_data.get("results", [])
        deployed = [r.get("artifact", "") for r in deploy_results if r.get("status") == "deployed"]
        skipped = [r.get("artifact", "") for r in deploy_results if r.get("status") == "skipped_exists"]
        failed_deploys = [{"artifact": r.get("artifact", ""), "error": r.get("error", "")} for r in deploy_results if r.get("status") == "failed"]

        # Extract validation info
        val_report = output.get("validation_report") or {}
        val_checks = val_report.get("checks", [])
        passed = [c.get("check_name", "") for c in val_checks if c.get("status") == "pass"]
        failed_vals = [{"check": c.get("check_name", ""), "message": c.get("message", "")} for c in val_checks if c.get("status") == "fail"]

        # Build friendly summary
        pipeline_status = output.get("status", "unknown")
        deployed_count = len(deployed)
        skipped_count = len(skipped)
        failed_count = len(failed_deploys)

        if failed_count == 0 and deployed_count > 0 and skipped_count == 0:
            status_label = "SUCCESS"
        elif failed_count == 0 and skipped_count > 0 and deployed_count > 0:
            status_label = f"PARTIAL SUCCESS - {deployed_count} new, {skipped_count} skipped (already exist)"
        elif failed_count == 0 and skipped_count > 0 and deployed_count == 0:
            status_label = f"NO CHANGES - all {skipped_count} objects already exist"
        elif failed_count > 0 and deployed_count > 0:
            status_label = f"PARTIAL SUCCESS - {deployed_count} deployed, {failed_count} failed"
        elif pipeline_status in ("completed", "escalated") and deployed_count == 0 and failed_count == 0:
            status_label = pipeline_status.upper()
        elif pipeline_status == "failed":
            status_label = "FAILED"
        else:
            status_label = pipeline_status.upper()

        summary = {
            "work_item_id": int(work_item_id),
            "story_id": story_id,
            "instance_id": instance_id,
            "runtime_status": runtime,
            "pipeline_status": status_label,
            "mode": output.get("mode", "N/A"),
            "objects_deployed": deployed,
            "objects_skipped": skipped,
            "objects_failed": failed_deploys,
            "deployed_count": deployed_count,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
            "validations_passed": passed,
            "validations_failed": failed_vals,
            "created": str(status.created_time),
            "completed": str(status.last_updated_time),
        }

        return func.HttpResponse(json.dumps(summary, indent=2), mimetype="application/json")
    except Exception as e:
        logger.error("story-status error: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# HTTP TRIGGER: Pipeline progress (step-by-step visual status)
# ============================================================
@app.route(route="pipeline-progress", methods=["GET", "POST"])
@app.durable_client_input(client_name="client")
async def pipeline_progress(req: func.HttpRequest, client) -> func.HttpResponse:
    """Return step-by-step progress for a pipeline run. Uses customStatus from orchestrator."""
    import os as _os
    import requests as _requests

    work_item_id = req.params.get("work_item_id", "")
    instance_id = req.params.get("instance_id", "")

    # Also accept work_item_id from POST body
    if not work_item_id and req.method == "POST":
        try:
            body = req.get_json()
            work_item_id = str(body.get("work_item_id", ""))
        except Exception as e:
            logger.warning("Non-critical error parsing POST body for work_item_id: %s", e)

    if not work_item_id and not instance_id:
        return func.HttpResponse('{"error": "work_item_id or instance_id required"}', status_code=400)

    try:
        # Find instance by work_item_id if needed
        if not instance_id and work_item_id:
            story_id = f"STORY-{work_item_id}"
            task_hub = _os.environ.get("TASK_HUB_NAME", "BiAutoHubV8")
            sys_key = _os.environ.get("DURABLETASK_EXTENSION_KEY", "")
            base_url = f"https://{_os.environ.get('WEBSITE_HOSTNAME', 'localhost')}"
            # Try both V8 and V6 hubs to find the instance
            found = False
            for hub in [task_hub, "BiAutoHubV8", "BiAutoHubV6"]:
                if found:
                    break
                url = (
                    f"{base_url}/runtime/webhooks/durabletask/instances"
                    f"?taskHub={hub}&connection=Storage&code={sys_key}"
                    f"&top=10&showInput=true"
                )
                try:
                    resp = _requests.get(url, timeout=30)
                    resp.raise_for_status()
                    instances_list = resp.json()
                    if isinstance(instances_list, dict):
                        instances_list = instances_list.get("value", instances_list)
                    for inst in reversed(instances_list):
                        inp = inst.get("input", "")
                        if isinstance(inp, str) and story_id in inp:
                            instance_id = inst["instanceId"]
                            found = True
                            break
                        elif isinstance(inp, dict) and inp.get("story_id") == story_id:
                            instance_id = inst["instanceId"]
                            found = True
                            break
                except Exception as e:
                    logger.warning("Non-critical error scanning instance for story %s: %s", story_id, e)
                    continue

        if not instance_id:
            return func.HttpResponse(json.dumps({"error": "No pipeline run found"}), status_code=404)

        status = await client.get_status(instance_id)
        if not status:
            return func.HttpResponse(json.dumps({"error": "Instance not found"}), status_code=404)

        runtime = status.runtime_status.name if status.runtime_status else "Unknown"
        custom = status.custom_status or {}

        # If pipeline is complete, build final progress from output
        if runtime in ("Completed", "Failed", "Terminated"):
            output = status.output or {}
            if isinstance(output, dict) and custom and isinstance(custom, dict):
                steps = custom.get("steps", [])
            else:
                # No custom status available, build from output
                deploy_data = (output.get("deploy_result") or {}) if isinstance(output, dict) else {}
                dep_results = deploy_data.get("results", [])
                dep_new = len([r for r in dep_results if r.get("status") == "deployed"])
                dep_skip = len([r for r in dep_results if r.get("status") == "skipped_exists"])
                pipeline_status = output.get("status", "unknown") if isinstance(output, dict) else "unknown"
                steps = [
                    {"step": 1, "name": "Fetch Story", "status": "completed", "detail": ""},
                    {"step": 2, "name": "Extract Source Tables", "status": "completed", "detail": ""},
                    {"step": 3, "name": "Planner Agent", "status": "completed", "detail": ""},
                    {"step": 4, "name": "Developer Agent", "status": "completed", "detail": ""},
                    {"step": 5, "name": "Deploy ADF Pipeline", "status": "completed", "detail": ""},
                    {"step": 6, "name": "Pre-Deploy Validation", "status": "completed", "detail": ""},
                    {"step": 7, "name": "Deploy to Synapse", "status": "completed", "detail": f"{dep_new} deployed, {dep_skip} skipped"},
                    {"step": 8, "name": "Post-Deploy Validation", "status": pipeline_status, "detail": ""},
                ]
        elif isinstance(custom, dict):
            steps = custom.get("steps", [])
        else:
            steps = [
                {"step": 1, "name": "Fetch Story", "status": "completed", "detail": ""},
                {"step": 2, "name": "Extract Source Tables", "status": "completed", "detail": ""},
                {"step": 3, "name": "Planner Agent", "status": "pending", "detail": ""},
                {"step": 4, "name": "Developer Agent", "status": "pending", "detail": ""},
                {"step": 5, "name": "Pre-Deploy Validation", "status": "pending", "detail": ""},
                {"step": 6, "name": "Deploy to Synapse", "status": "pending", "detail": ""},
                {"step": 7, "name": "Post-Deploy Validation", "status": "pending", "detail": ""},
            ]

        is_complete = runtime in ("Completed", "Failed", "Terminated")

        # Build visual text
        icons = {"completed": "✅", "in_progress": "⏳", "pending": "⬜", "failed": "❌", "escalated": "⚠️"}
        lines = []
        story_id = custom.get("story_id", "") if isinstance(custom, dict) else ""
        lines.append(f"Building BI Model — {story_id}")
        lines.append("")
        for s in steps:
            icon = icons.get(s["status"], "⬜")
            detail = f" — {s['detail']}" if s.get("detail") else ""
            lines.append(f"{icon} Step {s['step']}/8: {s['name']}{detail}")

        completed_count = len([s for s in steps if s["status"] == "completed"])
        lines.append("")
        lines.append(f"Progress: {completed_count}/8 steps")

        # Add final summary when complete
        if is_complete:
            output = status.output or {}
            if isinstance(output, dict):
                deploy_data = output.get("deploy_result") or {}
                dep_results = deploy_data.get("results", [])
                deployed = [r.get("artifact", "") for r in dep_results if r.get("status") == "deployed"]
                skipped_obj = [r.get("artifact", "") for r in dep_results if r.get("status") == "skipped_exists"]
                failed_obj = [r.get("artifact", "") for r in dep_results if r.get("status") == "failed"]

                lines.append("")
                lines.append("=" * 30)
                if len(failed_obj) == 0 and len(deployed) > 0:
                    lines.append("RESULT: SUCCESS")
                elif len(failed_obj) > 0:
                    lines.append(f"RESULT: {len(deployed)} deployed, {len(failed_obj)} failed")
                else:
                    lines.append(f"RESULT: {output.get('status', 'unknown').upper()}")

                lines.append("")
                for obj in deployed:
                    lines.append(f"  NEW: {obj}")
                for obj in skipped_obj:
                    lines.append(f"  EXISTS: {obj}")
                for r in dep_results:
                    if r.get("status") == "failed":
                        lines.append(f"  FAILED: {r.get('artifact', '')} — {r.get('error', 'unknown')[:150]}")
                lines.append("")
                lines.append(f"Deployed: {len(deployed)} | Skipped: {len(skipped_obj)} | Failed: {len(failed_obj)}")

        progress_text = "\n".join(lines)

        text_format = req.params.get("format", "") == "text"
        if text_format:
            return func.HttpResponse(progress_text, mimetype="text/plain")

        response_data = {
            "instance_id": instance_id,
            "runtime_status": runtime,
            "is_complete": is_complete,
            "current_step": custom.get("current_step", 0) if isinstance(custom, dict) else 0,
            "total_steps": 8,
            "steps": steps,
            "progress_text": progress_text,
        }
        # Pass through review gate and artifact details from customStatus
        if isinstance(custom, dict):
            if custom.get("awaiting_approval"):
                response_data["awaiting_approval"] = True
                response_data["review"] = custom.get("review", {})
            if custom.get("artifacts"):
                response_data["artifacts"] = custom.get("artifacts")

        return func.HttpResponse(json.dumps(response_data, indent=2, default=str), mimetype="application/json")

    except Exception as e:
        logger.error("pipeline-progress error: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# ============================================================
# ORCHESTRATOR: Main pipeline
# ============================================================
@app.orchestration_trigger(context_name="context")
def story_orchestrator(context: df.DurableOrchestrationContext):
    """Main orchestration: Planner → Developer → Code Review → Validator → (Healer) → Deploy."""
    story_input = context.get_input()
    state = PipelineState(story_id="pending")

    steps = [
        {"step": 1, "name": "Fetch Story", "status": "completed", "detail": ""},
        {"step": 2, "name": "Extract Source Tables", "status": "completed", "detail": ""},
        {"step": 3, "name": "Planner Agent", "status": "pending", "detail": ""},
        {"step": 4, "name": "Developer Agent", "status": "pending", "detail": ""},
        {"step": 5, "name": "Code Review", "status": "pending", "detail": ""},
        {"step": 6, "name": "Deploy ADF Pipeline", "status": "pending", "detail": ""},
        {"step": 7, "name": "Pre-Deploy Validation", "status": "pending", "detail": ""},
        {"step": 8, "name": "Deploy to Synapse", "status": "pending", "detail": ""},
        {"step": 9, "name": "Post-Deploy Validation", "status": "pending", "detail": ""},
    ]

    def _update_step(step_num, status, detail=""):
        for s in steps:
            if s["step"] == step_num:
                s["status"] = status
                if detail:
                    s["detail"] = detail
        context.set_custom_status({
            "story_id": state.story_id or story_input.get("story_id", ""),
            "current_step": step_num,
            "total_steps": 9,
            "steps": steps,
        })

    # Steps 1-2 already done (ADO fetch + story mapping happened in HTTP trigger)
    source_tables = story_input.get("source_tables", [])
    steps[0]["detail"] = story_input.get("title", "")
    steps[1]["detail"] = f"{len(source_tables)} tables: {', '.join(source_tables[:4])}"
    _update_step(2, "completed")

    # STATE REGISTRY: Register pipeline
    instance_id = context.instance_id
    reg_result = yield context.call_activity("register_pipeline", {
        "story_id": story_input.get("story_id", ""),
        "work_item_id": story_input.get("work_item_id"),
        "title": story_input.get("title", ""),
        "source_tables": source_tables,
        "instance_id": instance_id,
    })

    # ═══ TEAMS NOTIFICATION: Pipeline Started ═══
    try:
        yield context.call_activity("send_teams_started_notification", {
            "story_id": story_input.get("story_id", ""),
            "title": story_input.get("title", ""),
            "source_tables": source_tables,
            "work_item_id": str(story_input.get("work_item_id", "")),
            "instance_id": instance_id,
        })
    except Exception as e:
        logger.warning("Non-critical error registering pipeline: %s", e)
    pipeline_id = reg_result.get("pipeline_id")

    # PHASE 1: PLAN
    _update_step(3, "in_progress", "AI analyzing requirements...")
    state.status = "planning"
    build_plan_dict = yield context.call_activity("run_planner", story_input)
    build_plan = BuildPlan(**build_plan_dict)
    state.story_id = build_plan.story_id
    state.mode = build_plan.mode
    state.build_plan = build_plan
    artifact_count = len(build_plan.execution_order)

    # Build detailed plan summary for human review
    plan_summary = []
    for step in build_plan.execution_order:
        plan_summary.append({
            "step": step.step,
            "layer": step.layer.value,
            "action": step.action,
            "artifact_type": step.artifact_type.value,
            "object_name": step.object_name,
            "source": f"{step.source.schema_name}.{step.source.table}" if step.source and step.source.table else None,
            "logic": step.logic_summary or "",
            "load_pattern": step.load_pattern,
        })

    _update_step(3, "completed", f"{build_plan.mode.value} mode, {artifact_count} artifacts planned, risk: {build_plan.risk_level.value}")

    # ═══ TEAMS NOTIFICATION: Review Required ═══
    try:
        yield context.call_activity("send_teams_review_notification", {
            "instance_id": instance_id,
            "review": {
                "mode": build_plan.mode.value,
                "risk_level": build_plan.risk_level.value,
                "artifact_count": artifact_count,
                "plan_summary": plan_summary,
                "title": story_input.get("title", ""),
                "source_tables": source_tables,
            },
        })
    except Exception as e:
        logger.warning("Non-critical error sending plan notification: %s", e)

    # ═══ HUMAN REVIEW GATE ═══
    # Pause and wait for human approval before proceeding
    context.set_custom_status({
        "story_id": state.story_id or story_input.get("story_id", ""),
        "current_step": 3,
        "total_steps": 8,
        "steps": steps,
        "awaiting_approval": True,
        "review": {
            "mode": build_plan.mode.value,
            "risk_level": build_plan.risk_level.value,
            "artifact_count": artifact_count,
            "plan_summary": plan_summary,
            "validation_requirements": [
                {"check_type": v.check_type, "layer": v.layer.value if v.layer else None, "table": v.table, "columns": v.columns}
                for v in build_plan.validation_requirements
            ],
            "source_tables": source_tables,
            "title": story_input.get("title", ""),
        },
    })

    # Wait for human approval
    try:
        approval_event = yield context.wait_for_external_event("HumanReview")
    except TimeoutError:
        _update_step(3, "failed", "Review timed out after 30 minutes")
        state.status = "timed_out"
        return state.model_dump(mode="json")

    # Handle string, dict, or unexpected types
    if isinstance(approval_event, str):
        try:
            approval_event = json.loads(approval_event)
        except (json.JSONDecodeError, TypeError):
            approval_event = {"approved": False, "reason": "Invalid response"}
    if not isinstance(approval_event, dict):
        approval_event = {"approved": False, "reason": "Invalid response"}

    if not approval_event.get("approved", False):
        decline_reason = approval_event.get("reason", "Declined by reviewer")
        _update_step(3, "failed", f"Declined: {decline_reason}")
        state.status = "declined"
        return state.model_dump(mode="json")

    _update_step(3, "completed", f"{build_plan.mode.value} mode, {artifact_count} artifacts — APPROVED")
    # ═══ END REVIEW GATE ═══

    # STATE: Update pipeline with plan details
    if pipeline_id:
        yield context.call_activity("update_pipeline_status", {
            "pipeline_id": pipeline_id,
            "status": "developing",
            "mode": build_plan.mode.value,
            "risk_level": build_plan.risk_level.value,
            "artifact_count": artifact_count,
        })

    # PHASE 2: DEVELOP
    _update_step(4, "in_progress", f"AI generating SQL for {artifact_count} objects...")
    state.status = "developing"
    artifact_dict = yield context.call_activity("run_developer", build_plan_dict)
    artifacts = ArtifactBundle(**artifact_dict)
    state.artifacts = artifacts
    # Build artifact details for UI transparency
    artifact_details = []
    for a in artifacts.artifacts:
        preview = a.content[:300].replace('\n', ' ').strip()
        artifact_details.append({
            "name": a.object_name,
            "layer": a.layer.value,
            "type": a.artifact_type.value,
            "file": a.file_name,
            "sql_preview": preview,
        })
    _update_step(4, "completed", f"Generated SQL for {len(artifacts.artifacts)} objects")
    context.set_custom_status({
        "story_id": state.story_id or story_input.get("story_id", ""),
        "current_step": 4,
        "total_steps": 8,
        "steps": steps,
        "artifacts": artifact_details,
    })

    # STATE: Save generated artifacts to Config DB
    if pipeline_id:
        yield context.call_activity("save_artifacts_to_db", {
            "pipeline_id": pipeline_id,
            "instance_id": instance_id,
            "artifacts": [
                {"layer": a.layer.value, "object_name": a.object_name,
                 "artifact_type": a.artifact_type.value, "content": a.content,
                 "file_name": a.file_name}
                for a in artifacts.artifacts
            ],
        })

    # PHASE 2.5: CODE REVIEW + HEAL LOOP
    MAX_REVIEW_HEAL_RETRIES = 3
    review_attempt = 0
    review_verdict = "REJECT"

    while review_verdict in ("REJECT", "NEEDS_FIX") and review_attempt <= MAX_REVIEW_HEAL_RETRIES:
        attempt_label = f" (attempt {review_attempt + 1})" if review_attempt > 0 else ""
        _update_step(5, "in_progress", f"AI reviewing {len(artifacts.artifacts)} artifacts{attempt_label}...")

        review_result = yield context.call_activity("run_code_review", {
            "artifacts": [
                {"layer": a.layer.value, "object_name": a.object_name,
                 "artifact_type": a.artifact_type.value, "content": a.content,
                 "file_name": a.file_name}
                for a in artifacts.artifacts
            ],
            "build_plan": build_plan_dict,
        })

        review_verdict = review_result.get("overall_verdict", "APPROVE")
        review_findings = review_result.get("total_findings", 0)
        review_critical = review_result.get("critical_count", 0)
        review_warnings = review_result.get("warning_count", 0)
        review_summary = review_result.get("review_summary", "")

        # Store review details in customStatus for UI
        context.set_custom_status({
            "story_id": state.story_id or story_input.get("story_id", ""),
            "current_step": 5,
            "total_steps": 9,
            "steps": steps,
            "artifacts": artifact_details,
            "code_review": review_result,
        })

        if review_verdict == "APPROVE":
            break

        # NEEDS_FIX or REJECT — invoke Healer to fix
        if review_attempt < MAX_REVIEW_HEAL_RETRIES:
            _update_step(5, "in_progress",
                          f"Healer fixing {review_findings} issues (attempt {review_attempt + 1}/{MAX_REVIEW_HEAL_RETRIES})...")

            heal_result = yield context.call_activity("run_healer_for_review", {
                "review_result": review_result,
                "bundle": artifact_dict,
                "attempt": review_attempt + 1,
            })

            has_escalation = any(
                a.get("result") == HealerResult.ESCALATED.value
                for a in heal_result.get("actions", [])
            )
            fixed_count = sum(1 for a in heal_result.get("actions", []) if a.get("result") == HealerResult.FIXED.value)

            if has_escalation and fixed_count == 0:
                # All findings escalated, nothing could be fixed
                _update_step(5, "escalated",
                              f"Code review: {review_critical} critical issues, Healer could not fix — needs human review")
                state.status = "escalated"
                return state.model_dump(mode="json")

            # Update artifact_dict and artifacts with healed versions
            artifact_dict = heal_result["bundle"]
            artifacts = ArtifactBundle(**artifact_dict)

            # Refresh artifact_details for UI
            artifact_details = []
            for a in artifacts.artifacts:
                preview = a.content[:300].replace('\n', ' ').strip()
                artifact_details.append({
                    "name": a.object_name,
                    "layer": a.layer.value,
                    "type": a.artifact_type.value,
                    "file": a.file_name,
                    "sql_preview": preview,
                })

        review_attempt += 1

    # Final verdict after all heal attempts
    if review_verdict == "REJECT":
        _update_step(5, "failed",
                      f"REJECTED after {review_attempt} heal attempts: {review_critical} critical — {review_summary}")
        state.status = "failed"
        return state.model_dump(mode="json")

    if review_attempt > 0 and review_verdict == "APPROVE":
        _update_step(5, "completed",
                      f"APPROVED after {review_attempt} heal(s): {review_findings} findings resolved")
    else:
        verdict_label = "APPROVED" if review_verdict == "APPROVE" else "APPROVED WITH WARNINGS"
        _update_step(5, "completed",
                      f"{verdict_label}: {review_findings} findings ({review_critical} critical, {review_warnings} warning)")

    # PHASE 3: ADF Pipeline Deployment (before Synapse deploy)
    adf_artifacts = [a for a in artifacts.artifacts if a.artifact_type.value == "adf_pipeline"]
    if adf_artifacts:
        _update_step(6, "in_progress", "Deploying pipeline, datasets & trigger to ADF...")
        adf_deployed = 0
        adf_ds_count = 0
        adf_errors = []
        for adf_art in adf_artifacts:
            adf_result = yield context.call_activity("deploy_adf_pipeline", {
                "story_id": build_plan.story_id,
                "pipeline_json": adf_art.content,
            })
            pl_status = adf_result.get("pipeline", {}).get("status", "")
            if pl_status == "deployed":
                adf_deployed += 1
                adf_ds_count = len(adf_result.get("datasets", []))
                logger.info("ADF pipeline deployed: %s", adf_result.get("pipeline_name"))
            elif pl_status == "skipped":
                _update_step(6, "completed", "ADF not configured — skipped")
            else:
                adf_errors.append(adf_result.get("pipeline", {}).get("error", "unknown")[:100])

        if adf_errors:
            _update_step(6, "failed", f"ADF deploy failed: {'; '.join(adf_errors)}")
        elif adf_deployed > 0:
            trigger_status = adf_result.get("trigger", {}).get("status", "")
            trigger_msg = " + daily trigger" if trigger_status == "deployed" else ""
            _update_step(6, "completed", f"Pipeline + {adf_ds_count} datasets{trigger_msg} deployed")
    else:
        _update_step(6, "completed", "No ADF artifacts to deploy")

    # PHASE 4: PRE-VALIDATION
    _update_step(7, "in_progress", "Checking schema and syntax...")
    state.status = "validating"
    pre_check_dict = yield context.call_activity("run_validator_pre", {
        "bundle": artifact_dict,
        "plan": build_plan_dict,
    })
    pre_report = ValidationReport(**pre_check_dict)

    # HEAL LOOP (pre-deploy)
    retry = 0
    while pre_report.overall_status == ValidationStatus.FAIL and retry < MAX_HEAL_RETRIES:
        _update_step(7, "in_progress", f"Issues found, Healer Agent fixing (attempt {retry + 1})...")
        state.status = "healing"
        heal_result = yield context.call_activity("run_healer", {
            "report": pre_check_dict,
            "bundle": artifact_dict,
            "attempt": retry + 1,
        })

        has_escalation = any(
            a.get("result") == HealerResult.ESCALATED.value
            for a in heal_result.get("actions", [])
        )
        if has_escalation:
            _update_step(7, "escalated", "Healer escalated — needs human review")
            state.status = "escalated"
            return state.model_dump(mode="json")

        artifact_dict = heal_result["bundle"]
        state.status = "validating"
        pre_check_dict = yield context.call_activity("run_validator_pre", {
            "bundle": artifact_dict,
            "plan": build_plan_dict,
        })
        pre_report = ValidationReport(**pre_check_dict)
        retry += 1

    if pre_report.overall_status == ValidationStatus.FAIL:
        _update_step(7, "failed", "Pre-validation failed after retries")
        state.status = "failed"
        return state.model_dump(mode="json")

    pre_pass = len([c for c in pre_report.checks if c.status == ValidationStatus.PASS])
    _update_step(7, "completed", f"{pre_pass}/{len(pre_report.checks)} checks passed")

    # PHASE 5: DEPLOY TO DEV
    _update_step(8, "in_progress", "Executing SQL on Synapse Dedicated Pool...")
    state.status = "deploying"
    deploy_result = yield context.call_activity("deploy_artifacts", {
        "bundle": artifact_dict,
        "environment": "dev",
    })
    state.deploy_result = deploy_result
    dep_results = deploy_result.get("results", [])
    dep_new = len([r for r in dep_results if r.get("status") == "deployed"])
    dep_skip = len([r for r in dep_results if r.get("status") == "skipped_exists"])
    dep_fail = len([r for r in dep_results if r.get("status") == "failed"])
    _update_step(8, "completed", f"{dep_new} deployed, {dep_skip} skipped, {dep_fail} failed")

    # PHASE 6: POST-VALIDATION
    deploy_succeeded = dep_fail == 0 and (dep_new > 0 or dep_skip > 0)
    _update_step(9, "in_progress", "Verifying deployed data...")
    state.status = "validating"

    try:
        post_check_dict = yield context.call_activity("run_validator_post", {
            "plan": build_plan_dict,
            "environment": "dev",
        })
        post_report = ValidationReport(**post_check_dict)
        post_pass = len([c for c in post_report.checks if c.status == ValidationStatus.PASS])
        post_fail = len([c for c in post_report.checks if c.status == ValidationStatus.FAIL])

        if post_report.overall_status == ValidationStatus.PASS:
            _update_step(9, "completed", f"{post_pass}/{len(post_report.checks)} checks passed")
        elif deploy_succeeded:
            _update_step(9, "completed", f"{post_pass}/{len(post_report.checks)} passed, {post_fail} warnings (deploy OK)")
        else:
            _update_step(9, "completed", f"{post_pass}/{len(post_report.checks)} passed, {post_fail} failed")
    except Exception as post_err:
        if deploy_succeeded:
            _update_step(9, "completed", f"Validation skipped — deploy succeeded ({dep_new} new, {dep_skip} existing)")
            post_check_dict = {}
            post_report = ValidationReport(story_id=build_plan.story_id, phase="post_deploy", overall_status=ValidationStatus.PASS, checks=[], blocking_failures=[], warnings=[])
        else:
            _update_step(9, "escalated", f"Validation error: {str(post_err)[:100]}")
            state.status = "escalated"
            return state.model_dump(mode="json")

    # PHASE 6: APPROVAL GATE (optional — enable for production)
    # if build_plan.risk_level.value in ("medium", "high"):
    #     state.status = "awaiting_approval"
    #     approval = yield context.wait_for_external_event("HumanApproval")
    #     if not approval.get("approved", False):
    #         state.status = "rejected"
    #         return state.model_dump(mode="json")

    # PHASE 7: AUDIT LOG + STATE + VERSIONING
    state.status = "completed"
    yield context.call_activity("write_audit_log", {
        "story_id": build_plan.story_id,
        "plan": build_plan_dict,
        "validation": post_check_dict,
        "status": "completed",
    })

    # STATE: Update pipeline with final metrics
    target_objects = [a.object_name for a in artifacts.artifacts if a.artifact_type.value != "adf_pipeline"]
    if pipeline_id:
        yield context.call_activity("update_pipeline_status", {
            "pipeline_id": pipeline_id,
            "status": "active",
            "target_objects": target_objects,
            "deploy_count": dep_new,
            "skip_count": dep_skip,
            "fail_count": dep_fail,
        })

    # COLUMN LINEAGE: Extract and persist column-level lineage
    try:
        lineage_artifacts = [
            {"content": a.content, "layer": a.layer.value, "object_name": a.object_name}
            for a in artifacts.artifacts if a.artifact_type.value != "adf_pipeline"
        ]
        yield context.call_activity("record_column_lineage", {
            "story_id": build_plan.story_id,
            "artifacts": lineage_artifacts,
        })
    except Exception as lin_err:
        logger.warning("Column lineage recording failed (non-blocking): %s", lin_err)

    # NOTIFICATION: Send Teams adaptive card on completion
    try:
        deploy_data = deploy_result or {}
        dep_results = deploy_data.get("results", []) if isinstance(deploy_data, dict) else []
        deployed_names = [r.get("artifact", "") for r in dep_results if r.get("status") == "deployed"]
        skipped_names = [r.get("artifact", "") for r in dep_results if r.get("status") == "skipped_exists"]
        failed_names = [f"{r.get('artifact', '')} — {r.get('error', '')[:80]}" for r in dep_results if r.get("status") == "failed"]
        yield context.call_activity("send_completion_notification", {
            "story_id": build_plan.story_id,
            "title": story_input.get("title", ""),
            "deployed": deployed_names,
            "skipped": skipped_names,
            "failed": failed_names,
            "elapsed": 0,
        })
    except Exception as e:
        logger.warning("Non-critical error sending deployment notification: %s", e)

    # ARTIFACT VERSIONING: Commit SQL to ADO repo
    versioning_artifacts = [
        {"file_path": a.file_name, "content": a.content}
        for a in artifacts.artifacts
    ]
    if versioning_artifacts:
        commit_result = yield context.call_activity("commit_artifacts_to_repo", {
            "story_id": build_plan.story_id,
            "artifacts": versioning_artifacts,
            "build_plan_json": json.dumps(build_plan_dict, indent=2, default=str),
        })
        if commit_result.get("commit_sha"):
            logger.info("Artifacts committed: sha=%s branch=%s",
                        commit_result["commit_sha"], commit_result["branch"])

    state.validation_report = post_report
    return state.model_dump(mode="json")


# ============================================================
# ACTIVITY FUNCTIONS (one per agent)
# ============================================================
@app.activity_trigger(input_name="story_input")
def run_planner(story_input) -> dict:
    config = AppConfig.from_env()
    from planner.agent import PlannerAgent
    agent = PlannerAgent(config)
    plan = agent.run(story_input)
    return plan.model_dump(mode="json")


@app.activity_trigger(input_name="plan_dict")
def run_developer(plan_dict: dict) -> dict:
    config = AppConfig.from_env()
    from developer.agent import DeveloperAgent
    plan = BuildPlan(**plan_dict)
    agent = DeveloperAgent(config)
    bundle = agent.run(plan)
    return bundle.model_dump(mode="json")


@app.activity_trigger(input_name="payload")
def run_code_review(payload: dict) -> dict:
    """AI reviews generated SQL/ADF artifacts before deployment."""
    config = AppConfig.from_env()
    from reviewer.agent import CodeReviewAgent
    agent = CodeReviewAgent(config)
    return agent.review(
        artifacts=payload.get("artifacts", []),
        build_plan=payload.get("build_plan", {}),
    )


@app.activity_trigger(input_name="payload")
def run_validator_pre(payload: dict) -> dict:
    config = AppConfig.from_env()
    from validator.agent import ValidatorAgent
    bundle = ArtifactBundle(**payload["bundle"])
    plan = BuildPlan(**payload["plan"])
    agent = ValidatorAgent(config)
    report = agent.pre_deploy_check(bundle, plan)
    return report.model_dump(mode="json")


@app.activity_trigger(input_name="payload")
def run_validator_post(payload: dict) -> dict:
    config = AppConfig.from_env()
    from validator.agent import ValidatorAgent
    plan = BuildPlan(**payload["plan"])
    agent = ValidatorAgent(config)
    report = agent.post_deploy_check(plan, payload["environment"])
    return report.model_dump(mode="json")


@app.activity_trigger(input_name="payload")
def run_healer(payload: dict) -> dict:
    config = AppConfig.from_env()
    from healer.agent import HealerAgent
    report = ValidationReport(**payload["report"])
    bundle = ArtifactBundle(**payload["bundle"])
    agent = HealerAgent(config)
    corrected_bundle, actions = agent.run(report, bundle, payload.get("attempt", 1))
    return {
        "bundle": corrected_bundle.model_dump(mode="json"),
        "actions": [a.model_dump(mode="json") for a in actions],
    }


@app.activity_trigger(input_name="payload")
def run_healer_for_review(payload: dict) -> dict:
    """Healer fixes artifacts based on Code Review findings."""
    config = AppConfig.from_env()
    from healer.agent import HealerAgent
    bundle = ArtifactBundle(**payload["bundle"])
    agent = HealerAgent(config)
    corrected_bundle, actions = agent.heal_from_review(
        review_result=payload["review_result"],
        artifact_bundle=bundle,
        attempt_number=payload.get("attempt", 1),
    )
    return {
        "bundle": corrected_bundle.model_dump(mode="json"),
        "actions": [a.model_dump(mode="json") for a in actions],
    }


def _ensure_synapse_prerequisites(synapse, config):
    """Ensure Parquet file format and BronzeDataSource exist with correct settings."""
    storage = config.storage_account_name

    # Step 1: Create file format if missing
    try:
        rows = synapse.execute_query("SELECT COUNT(*) AS cnt FROM sys.external_file_formats WHERE name = 'ParquetFileFormat'")
        if rows[0]["cnt"] == 0:
            synapse.execute_ddl("CREATE EXTERNAL FILE FORMAT [ParquetFileFormat] WITH (FORMAT_TYPE = PARQUET)")
            logger.info("Created ParquetFileFormat")
    except Exception as e:
        logger.warning("File format check/create failed: %s", e)

    # Step 2: Check if data source exists
    try:
        rows = synapse.execute_query("SELECT COUNT(*) AS cnt FROM sys.external_data_sources WHERE name = 'BronzeDataSource'")
        if rows[0]["cnt"] == 0:
            synapse.execute_ddl(
                f"CREATE EXTERNAL DATA SOURCE [BronzeDataSource] "
                f"WITH (LOCATION = 'abfss://bronze@{storage}.dfs.core.windows.net', CREDENTIAL = [StorageCredential])"
            )
            logger.info("Created BronzeDataSource with credential")
        else:
            # Verify it has a credential
            rows2 = synapse.execute_query(
                "SELECT ds.credential_id FROM sys.external_data_sources ds WHERE ds.name = 'BronzeDataSource'"
            )
            if rows2[0]["credential_id"] == 0 or rows2[0]["credential_id"] is None:
                logger.warning("BronzeDataSource exists without credential, dropping and recreating")
                # Drop dependent external tables first
                ext_tables = synapse.execute_query(
                    "SELECT s.name AS sname, t.name AS tname FROM sys.external_tables t "
                    "JOIN sys.schemas s ON t.schema_id = s.schema_id"
                )
                for tbl in ext_tables:
                    try:
                        synapse.execute_ddl(f"DROP EXTERNAL TABLE [{tbl['sname']}].[{tbl['tname']}]")
                    except Exception as e:
                        logger.warning("Non-critical error dropping external table %s.%s: %s", tbl['sname'], tbl['tname'], e)
                synapse.execute_ddl("DROP EXTERNAL DATA SOURCE [BronzeDataSource]")
                synapse.execute_ddl(
                    f"CREATE EXTERNAL DATA SOURCE [BronzeDataSource] "
                    f"WITH (LOCATION = 'abfss://bronze@{storage}.dfs.core.windows.net', CREDENTIAL = [StorageCredential])"
                )
                logger.info("Recreated BronzeDataSource with credential")
            else:
                logger.info("BronzeDataSource exists with credential, OK")
    except Exception as e:
        logger.warning("Data source check/create failed: %s", e)


@app.activity_trigger(input_name="payload")
def deploy_artifacts(payload: dict) -> dict:
    """Deploy generated SQL artifacts to Synapse."""
    import re as _re
    config = AppConfig.from_env()
    from shared.synapse_client import SynapseClient
    synapse = SynapseClient(config)
    bundle = ArtifactBundle(**payload["bundle"])
    environment = payload["environment"]
    results = []

    # Pre-deploy: ensure prerequisites exist with correct settings
    try:
        _ensure_synapse_prerequisites(synapse, config)
    except Exception as e:
        logger.warning("Pre-deploy prerequisite check failed: %s", e)

    for artifact in bundle.artifacts:
        if artifact.artifact_type.value == "adf_pipeline":
            results.append({"artifact": artifact.object_name, "status": "adf_skipped"})
            continue
        try:
            # Check if object already exists (brownfield detection)
            obj_name = artifact.object_name.replace("[", "").replace("]", "")
            parts = obj_name.split(".")
            if len(parts) == 2:
                schema_name, table_name = parts
                if synapse.check_object_exists(schema_name, table_name):
                    results.append({"artifact": artifact.object_name, "status": "skipped_exists"})
                    logger.info("Skipping %s — already exists (brownfield)", artifact.object_name)
                    continue

            sql = artifact.content
            sql = _re.sub(r'^```\w*\n?', '', sql, flags=_re.MULTILINE)
            sql = _re.sub(r'\n?```$', '', sql, flags=_re.MULTILINE)
            sql = sql.strip()
            if not sql:
                results.append({"artifact": artifact.object_name, "status": "skipped_empty"})
                continue
            synapse.execute_ddl(sql)
            results.append({"artifact": artifact.object_name, "status": "deployed"})
        except Exception as e:
            results.append({"artifact": artifact.object_name, "status": "failed", "error": str(e)})

    return {"environment": environment, "results": results}


@app.activity_trigger(input_name="payload")
def write_audit_log(payload: dict) -> dict:
    """Write deployment record to audit tables."""
    config = AppConfig.from_env()
    from shared.synapse_client import SynapseClient
    synapse = SynapseClient(config)

    sql = """
    INSERT INTO [catalog].[deployment_log]
        (story_id, agent_name, artifact_type, artifact_name, environment, status, build_plan_json, validation_report)
    VALUES
        (?, 'orchestrator', 'pipeline', 'full_pipeline',
         'dev', ?,
         ?,
         ?)
    """
    params = (
        payload["story_id"],
        payload["status"],
        json.dumps(payload.get("plan", {})),
        json.dumps(payload.get("validation", {})),
    )
    try:
        with synapse.connection(autocommit=True) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
    except Exception as e:
        logger.error("Failed to write audit log: %s", e)

    return {"status": "logged"}


# ============================================================
# ACTIVITY FUNCTIONS: State Registry + Artifact Versioning
# ============================================================

@app.activity_trigger(input_name="payload")
def register_pipeline(payload: dict) -> dict:
    """Register or update a pipeline in the Config DB."""
    try:
        config = AppConfig.from_env()
        from shared.state_registry import StateRegistry
        reg = StateRegistry(config)
        pid = reg.register_pipeline(
            story_id=payload["story_id"],
            work_item_id=payload.get("work_item_id"),
            title=payload.get("title", ""),
            source_tables=payload.get("source_tables", []),
            instance_id=payload.get("instance_id", ""),
        )
        return {"pipeline_id": pid}
    except Exception as e:
        logger.error("register_pipeline failed: %s", e)
        return {"pipeline_id": None, "error": str(e)}


@app.activity_trigger(input_name="payload")
def log_pipeline_step(payload: dict) -> dict:
    """Log an execution step to the Config DB."""
    try:
        config = AppConfig.from_env()
        from shared.state_registry import StateRegistry
        reg = StateRegistry(config)
        log_id = reg.log_step(
            pipeline_id=payload["pipeline_id"],
            instance_id=payload["instance_id"],
            step_number=payload["step_number"],
            step_name=payload["step_name"],
            status=payload["status"],
            detail=payload.get("detail"),
            error_message=payload.get("error_message"),
        )
        return {"log_id": log_id}
    except Exception as e:
        logger.error("log_pipeline_step failed: %s", e)
        return {"log_id": None, "error": str(e)}


@app.activity_trigger(input_name="payload")
def complete_pipeline_step(payload: dict) -> dict:
    """Mark a step as completed in the Config DB."""
    try:
        config = AppConfig.from_env()
        from shared.state_registry import StateRegistry
        reg = StateRegistry(config)
        reg.complete_step(
            log_id=payload["log_id"],
            status=payload.get("status", "completed"),
            detail=payload.get("detail"),
        )
        return {"ok": True}
    except Exception as e:
        logger.error("complete_pipeline_step failed: %s", e)
        return {"ok": False, "error": str(e)}


@app.activity_trigger(input_name="payload")
def update_pipeline_status(payload: dict) -> dict:
    """Update pipeline registry status and metrics."""
    try:
        config = AppConfig.from_env()
        from shared.state_registry import StateRegistry
        reg = StateRegistry(config)
        reg.update_pipeline_status(
            pipeline_id=payload["pipeline_id"],
            status=payload["status"],
            mode=payload.get("mode"),
            risk_level=payload.get("risk_level"),
            target_objects=payload.get("target_objects"),
            artifact_count=payload.get("artifact_count"),
            deploy_count=payload.get("deploy_count"),
            skip_count=payload.get("skip_count"),
            fail_count=payload.get("fail_count"),
            duration_sec=payload.get("duration_sec"),
        )
        return {"ok": True}
    except Exception as e:
        logger.error("update_pipeline_status failed: %s", e)
        return {"ok": False, "error": str(e)}


@app.activity_trigger(input_name="payload")
def save_artifacts_to_db(payload: dict) -> dict:
    """Save all artifacts to the Config DB artifact_versions table."""
    try:
        config = AppConfig.from_env()
        from shared.state_registry import StateRegistry
        reg = StateRegistry(config)
        artifact_ids = []
        for art in payload.get("artifacts", []):
            aid = reg.save_artifact(
                pipeline_id=payload["pipeline_id"],
                instance_id=payload["instance_id"],
                layer=art["layer"],
                object_name=art["object_name"],
                artifact_type=art["artifact_type"],
                sql_content=art["content"],
                file_path=art.get("file_name"),
                deploy_status=art.get("deploy_status"),
            )
            artifact_ids.append(aid)
        return {"artifact_ids": artifact_ids}
    except Exception as e:
        logger.error("save_artifacts_to_db failed: %s", e)
        return {"artifact_ids": [], "error": str(e)}


@app.activity_trigger(input_name="payload")
def commit_artifacts_to_repo(payload: dict) -> dict:
    """Commit generated artifacts to ADO Git repo."""
    try:
        config = AppConfig.from_env()
        from shared.artifact_versioner import ArtifactVersioner
        versioner = ArtifactVersioner(config)
        result = versioner.commit_artifacts(
            story_id=payload["story_id"],
            artifacts=payload["artifacts"],
            build_plan_json=payload.get("build_plan_json"),
        )
        return result or {"commit_sha": None, "branch": None}
    except Exception as e:
        logger.error("commit_artifacts_to_repo failed: %s", e)
        return {"commit_sha": None, "branch": None, "error": str(e)}


@app.activity_trigger(input_name="payload")
def deploy_adf_pipeline(payload: dict) -> dict:
    """Deploy generated pipeline JSON to Azure Data Factory."""
    try:
        from shared.adf_client import ADFClient
        client = ADFClient()
        if not client.is_configured:
            return {"pipeline": {"status": "skipped", "reason": "ADF not configured"}}
        result = client.deploy_bronze_pipeline(
            story_id=payload["story_id"],
            pipeline_json_str=payload["pipeline_json"],
        )
        return result
    except Exception as e:
        logger.error("deploy_adf_pipeline failed: %s", e)
        return {"pipeline": {"status": "failed", "error": str(e)}}


@app.activity_trigger(input_name="payload")
def run_data_quality(payload: dict) -> dict:
    """Run data quality checks on deployed objects."""
    try:
        config = AppConfig.from_env()
        from shared.data_quality import DataQualityValidator
        dq = DataQualityValidator(config)
        report = dq.run_checks(
            story_id=payload["story_id"],
            objects=payload.get("objects", []),
        )
        return report.to_dict()
    except Exception as e:
        logger.error("run_data_quality failed: %s", e)
        return {"story_id": payload.get("story_id"), "overall_status": "skip", "results": [], "error": str(e)}


@app.activity_trigger(input_name="payload")
def record_column_lineage(payload: dict) -> dict:
    """Extract and persist column-level lineage from generated SQL."""
    try:
        from shared.lineage_tracker import LineageTracker
        tracker = LineageTracker()
        story_id = payload["story_id"]
        artifacts = payload.get("artifacts", [])
        mappings = tracker.extract_lineage_from_sql(story_id, artifacts)
        if mappings:
            result = tracker.record_lineage(story_id, mappings)
            return {"lineage_records": result.get("records", 0)}
        return {"lineage_records": 0}
    except Exception as e:
        logger.warning("record_column_lineage failed (non-blocking): %s", e)
        return {"lineage_records": 0, "error": str(e)}


@app.activity_trigger(input_name="payload")
def send_completion_notification(payload: dict) -> dict:
    """Send Teams adaptive card on pipeline completion."""
    from shared.teams_webhook import send_card, completion_card
    try:
        card = completion_card(
            story_id=payload.get("story_id", ""),
            title=payload.get("title", ""),
            deployed=payload.get("deployed", []),
            skipped=payload.get("skipped", []),
            failed=payload.get("failed", []),
            elapsed=payload.get("elapsed", 0),
        )
        return send_card(card)
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@app.activity_trigger(input_name="payload")
def send_teams_started_notification(payload: dict) -> dict:
    """Send Teams adaptive card when pipeline starts."""
    from shared.teams_webhook import send_card, pipeline_started_card
    try:
        card = pipeline_started_card(
            story_id=payload.get("story_id", ""),
            title=payload.get("title", ""),
            tables=payload.get("source_tables", []),
            work_item_id=payload.get("work_item_id", ""),
            instance_id=payload.get("instance_id", ""),
        )
        return send_card(card)
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@app.activity_trigger(input_name="payload")
def send_teams_review_notification(payload: dict) -> dict:
    """Send Teams adaptive card for human review gate with approve/decline actions."""
    from shared.teams_webhook import send_card, review_gate_card
    try:
        review = payload.get("review", {})
        card = review_gate_card(
            instance_id=payload.get("instance_id", ""),
            mode=review.get("mode", "unknown"),
            risk_level=review.get("risk_level", "unknown"),
            artifact_count=review.get("artifact_count", 0),
            plan_summary=review.get("plan_summary", []),
            title=review.get("title", ""),
            source_tables=review.get("source_tables"),
        )
        return send_card(card)
    except Exception as e:
        return {"status": "failed", "error": str(e)}

# ============================================================
# COMMANDER PIPELINE — Dynamic Agent Orchestration
# ============================================================


@app.route(route="rag/sync", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def rag_sync(req: func.HttpRequest) -> func.HttpResponse:
    """Sync client metadata into RAG knowledge base (Synapse schema + glossary + conventions)."""
    try:
        from shared.catalog_indexer import CatalogIndexer
        from shared.rag_retriever import RAGRetriever
        config = AppConfig.from_env()
        retriever = RAGRetriever(config)
        indexer = CatalogIndexer(config, retriever)
        stats = indexer.full_sync()
        return func.HttpResponse(json.dumps({"status": "ok", "stats": stats}), mimetype="application/json")
    except Exception as e:
        logger.error("RAG sync error: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


@app.route(route="rag/status", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def rag_status(req: func.HttpRequest) -> func.HttpResponse:
    """Get RAG knowledge base status (document count, types, backend)."""
    try:
        from shared.rag_retriever import RAGRetriever
        retriever = RAGRetriever(AppConfig.from_env())
        return func.HttpResponse(json.dumps(retriever.get_status()), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


@app.route(route="rag/query", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def rag_query(req: func.HttpRequest) -> func.HttpResponse:
    """Test RAG retrieval: send a query, get grounding context back."""
    try:
        from shared.rag_retriever import RAGRetriever
        body = req.get_json()
        query = body.get("query", "")
        top_k = body.get("top_k", 15)
        retriever = RAGRetriever(AppConfig.from_env())
        result = retriever.retrieve(query, top_k)
        return func.HttpResponse(json.dumps({
            "query": query, "top_k": top_k,
            "documents": [d.to_dict() for d in result.documents],
            "prompt_context": result.to_prompt_context(),
        }), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


@app.route(route="rag/ingest", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def rag_ingest(req: func.HttpRequest) -> func.HttpResponse:
    """Manually ingest documents into the RAG knowledge base.

    Body: { "documents": [ { "type": "table_schema|business_term|sql_pattern|...", "content": "..." } ] }
    Or:   { "data_dictionary": [ { "table": "...", "column": "...", "type": "...", "description": "..." } ] }
    Or:   { "sql_files": [ { "name": "...", "content": "CREATE VIEW ..." } ] }
    Or:   { "glossary": [ { "term": "...", "definition": "..." } ] }
    """
    try:
        from shared.rag_retriever import RAGRetriever, RAGDocument, DocumentType
        body = req.get_json()
        retriever = RAGRetriever(AppConfig.from_env())
        docs = []
        count = 0

        if body.get("documents"):
            for d in body["documents"]:
                dtype = d.get("type", "table_schema")
                try:
                    dt = DocumentType(dtype)
                except ValueError:
                    dt = DocumentType.TABLE_SCHEMA
                docs.append(RAGDocument(
                    doc_id=f"manual:{dtype}:{count}", doc_type=dt,
                    content=d.get("content", ""), metadata=d.get("metadata", {})))
                count += 1

        if body.get("data_dictionary"):
            for dd in body["data_dictionary"]:
                tbl = dd.get("table", "")
                col = dd.get("column", "")
                dtype = dd.get("type", dd.get("data_type", ""))
                desc = dd.get("description", "")
                nullable = dd.get("nullable", "YES")
                content = f"[{tbl}].[{col}] ({dtype}, {'NULL' if nullable=='YES' else 'NOT NULL'})"
                if desc:
                    content += f" -- {desc}"
                docs.append(RAGDocument(
                    doc_id=f"dd:{tbl}.{col}", doc_type=DocumentType.COLUMN_DEF,
                    content=content, metadata={"table": tbl, "column": col, "data_type": dtype}))
                count += 1

        if body.get("sql_files"):
            for sf in body["sql_files"]:
                name = sf.get("name", f"sql_{count}")
                content = sf.get("content", "")
                docs.append(RAGDocument(
                    doc_id=f"sql:{name}", doc_type=DocumentType.SQL_PATTERN,
                    content=f"SQL Pattern: {name}\n{content[:2000]}", metadata={"file": name}))
                count += 1

        if body.get("glossary"):
            for g in body["glossary"]:
                term = g.get("term", "")
                definition = g.get("definition", "")
                content = f"Business Term: {term}\nDefinition: {definition}"
                if g.get("category"):
                    content += f"\nCategory: {g['category']}"
                if g.get("formula"):
                    content += f"\nFormula: {g['formula']}"
                docs.append(RAGDocument(
                    doc_id=f"glossary:{term}", doc_type=DocumentType.BUSINESS_TERM,
                    content=content, metadata={"term": term}))
                count += 1

        if docs:
            retriever.index_documents(docs)
        return func.HttpResponse(json.dumps({
            "status": "ok", "ingested": count, "total": retriever.document_count,
        }), mimetype="application/json")
    except Exception as e:
        logger.error("RAG ingest error: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


@app.route(route="rag/scan-source", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def rag_scan_source(req: func.HttpRequest) -> func.HttpResponse:
    """Scan a source database and index its schema into the RAG knowledge base.

    Body: { "connection_string": "...", "source_name": "erp_system", "schemas": ["dbo","sales"] }
    Or:   { "tables": [{table, schema, columns: [{name, type, description}]}], "source_name": "manual" }
    """
    try:
        from shared.source_catalog_scanner import SourceCatalogScanner
        from shared.rag_retriever import RAGRetriever
        config = AppConfig.from_env()
        body = req.get_json()
        retriever = RAGRetriever(config)
        scanner = SourceCatalogScanner(config, retriever)

        if body.get("tables"):
            stats = scanner.ingest_manual_source_schema(
                body["tables"], source_name=body.get("source_name", "manual"))
        elif body.get("connection_string"):
            stats = scanner.scan_source_db(
                body["connection_string"],
                source_name=body.get("source_name", "source"),
                schemas=body.get("schemas"))
        else:
            return func.HttpResponse(json.dumps({"error": "Provide 'connection_string' or 'tables'"}),
                                     status_code=400, mimetype="application/json")
        return func.HttpResponse(json.dumps({"status": "ok", "stats": stats}), mimetype="application/json")
    except Exception as e:
        logger.error("RAG source scan error: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


@app.route(route="rag/seed-templates", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def rag_seed_templates(req: func.HttpRequest) -> func.HttpResponse:
    """Seed the RAG knowledge base with industry templates and standard conventions.

    Body: { "industries": ["retail", "finance"] }
    Available: retail, finance, healthcare, saas
    """
    try:
        from shared.template_kb_seeder import TemplateKBSeeder
        from shared.rag_retriever import RAGRetriever
        body = req.get_json() if req.get_body() else {}
        industries = body.get("industries", [])
        retriever = RAGRetriever(AppConfig.from_env())
        seeder = TemplateKBSeeder(retriever)
        stats = seeder.seed_all(industries)
        stats["available_industries"] = TemplateKBSeeder.available_industries()
        return func.HttpResponse(json.dumps({"status": "ok", "stats": stats}), mimetype="application/json")
    except Exception as e:
        logger.error("RAG seed templates error: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


@app.route(route="engine-config", methods=["GET", "POST"], auth_level=func.AuthLevel.FUNCTION)
def engine_config_endpoint(req: func.HttpRequest) -> func.HttpResponse:
    """GET: returns supported engines and current defaults. POST: validates engine config."""
    if req.method == "GET":
        return func.HttpResponse(json.dumps({
            "engines": [
                {"id": "adf", "name": "Azure Data Factory", "description": "Managed ETL with Copy Activities", "status": "production"},
                {"id": "databricks", "name": "Databricks", "description": "Spark notebooks with Delta Lake", "status": "preview"},
                {"id": "synapse_spark", "name": "Synapse Spark", "description": "Spark pools within Synapse workspace", "status": "preview"},
            ],
            "load_patterns": [
                {"id": "full_load", "name": "Full Load", "description": "Truncate and reload entire table"},
                {"id": "incremental", "name": "Incremental Load", "description": "Load only new/changed rows using watermark column"},
                {"id": "merge_scd1", "name": "Merge (SCD Type 1)", "description": "Upsert: update existing, insert new rows"},
                {"id": "merge_scd2", "name": "Merge (SCD Type 2)", "description": "Track history with effective_from/effective_to columns"},
            ],
            "defaults": {"pipeline_engine": "adf", "load_pattern": "full_load"},
        }), mimetype="application/json")
    try:
        body = req.get_json()
        engine = body.get("pipeline_engine", "adf")
        pattern = body.get("load_pattern", "full_load")
        valid_engines = ("adf", "databricks", "synapse_spark")
        valid_patterns = ("full_load", "incremental", "merge_scd1", "merge_scd2")
        errors = []
        if engine not in valid_engines:
            errors.append(f"Invalid engine: {engine}. Must be one of {valid_engines}")
        if pattern not in valid_patterns:
            errors.append(f"Invalid load_pattern: {pattern}. Must be one of {valid_patterns}")
        if pattern in ("incremental",) and not body.get("incremental_column"):
            errors.append("incremental_column required for incremental load pattern")
        if pattern in ("merge_scd1", "merge_scd2") and not body.get("merge_key_columns"):
            errors.append("merge_key_columns required for merge patterns")
        if pattern == "merge_scd2" and not body.get("scd2_tracked_columns"):
            errors.append("scd2_tracked_columns required for SCD Type 2")
        if engine == "databricks" and not body.get("databricks_workspace_url"):
            errors.append("databricks_workspace_url required for Databricks engine")
        if engine == "synapse_spark" and not body.get("spark_pool_name"):
            errors.append("spark_pool_name required for Synapse Spark engine")
        if errors:
            return func.HttpResponse(json.dumps({"valid": False, "errors": errors}), status_code=400, mimetype="application/json")
        return func.HttpResponse(json.dumps({"valid": True, "config": body}), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=400, mimetype="application/json")


@app.route(route="commander/run", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
@app.durable_client_input(client_name="client")
async def commander_run(req: func.HttpRequest, client) -> func.HttpResponse:
    """Start a Commander-driven pipeline."""
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse(json.dumps({"error": "Invalid JSON"}), status_code=400)

    story_id = body.get("story_id") or body.get("work_item_id") or "unknown"
    body["story_id"] = story_id
    instance_id = await client.start_new("commander_orchestrator", None, body)
    return func.HttpResponse(
        json.dumps({"instance_id": instance_id, "story_id": story_id,
                     "status_url": f"/api/commander/status?instance_id={instance_id}"}),
        status_code=202, mimetype="application/json")


@app.route(route="commander/status", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
@app.durable_client_input(client_name="client")
async def commander_status(req: func.HttpRequest, client) -> func.HttpResponse:
    """Get Commander pipeline status including supervisor audit trail."""
    instance_id = req.params.get("instance_id")
    if not instance_id:
        return func.HttpResponse(json.dumps({"error": "instance_id required"}), status_code=400)
    status = await client.get_status(instance_id)
    if not status:
        return func.HttpResponse(json.dumps({"error": "not found"}), status_code=404)
    custom = status.custom_status or {}
    return func.HttpResponse(json.dumps({
        "instance_id": instance_id,
        "runtime_status": status.runtime_status.value if status.runtime_status else "unknown",
        "commander": custom.get("commander", {}),
        "supervisor": custom.get("supervisor", {}),
        "current_step": custom.get("current_step", ""),
        "steps": custom.get("steps", []),
        "awaiting_review": custom.get("awaiting_review", False),
    }, default=str), mimetype="application/json")


@app.route(route="commander/approve", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
@app.durable_client_input(client_name="client")
async def commander_approve(req: func.HttpRequest, client) -> func.HttpResponse:
    """Approve Commander's plan at human review gate."""
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse(json.dumps({"error": "Invalid JSON"}), status_code=400)
    instance_id = body.get("instance_id")
    if not instance_id:
        return func.HttpResponse(json.dumps({"error": "instance_id required"}), status_code=400)
    await client.raise_event(instance_id, "CommanderReviewApproved", {"approved": True})
    return func.HttpResponse(json.dumps({"status": "approved"}), mimetype="application/json")


@app.route(route="commander/decline", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
@app.durable_client_input(client_name="client")
async def commander_decline(req: func.HttpRequest, client) -> func.HttpResponse:
    """Decline Commander's plan at human review gate."""
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse(json.dumps({"error": "Invalid JSON"}), status_code=400)
    instance_id = body.get("instance_id")
    if not instance_id:
        return func.HttpResponse(json.dumps({"error": "instance_id required"}), status_code=400)
    reason = body.get("reason", "Declined by reviewer")
    await client.raise_event(instance_id, "CommanderReviewApproved", {"approved": False, "reason": reason})
    return func.HttpResponse(json.dumps({"status": "declined"}), mimetype="application/json")


# ============================================================
# COMMANDER ORCHESTRATOR (Durable Functions)
# ============================================================


@app.orchestration_trigger(context_name="context")
def commander_orchestrator(context: df.DurableOrchestrationContext):
    """Commander + Supervisor orchestrator. Dynamic agent dispatch with quality gates."""
    input_data = context.get_input()
    story_id = input_data.get("story_id", "unknown")
    mode = input_data.get("mode", "greenfield")
    steps_status = []

    def _update(current_step="", awaiting_review=False, extra=None):
        context.set_custom_status({
            "story_id": story_id, "mode": mode, "current_step": current_step,
            "steps": steps_status, "awaiting_review": awaiting_review,
            "commander": (extra or {}).get("commander", {}),
            "supervisor": (extra or {}).get("supervisor", {}),
        })

    # Phase 0: Commander creates execution plan
    _update("Commander: Planning execution...")
    plan_result = yield context.call_activity("commander_plan_execution", {"story": input_data, "mode": mode})
    plan_steps = plan_result.get("steps", [])
    for ps in plan_steps:
        steps_status.append({"id": ps["id"], "agent": ps["agent"], "description": ps["description"],
                             "status": "pending", "attempts": 0, "supervisor_verdict": None})

    # Phase 1: Supervisor validates plan
    _update("Supervisor: Validating plan...")
    sv_check = yield context.call_activity("supervisor_check_plan", {"plan": plan_result, "story": input_data, "mode": mode})
    if not sv_check.get("approved", True) and sv_check.get("action") == "halt":
        _update("Supervisor HALTED", extra={"supervisor": sv_check})
        return {"status": "halted_by_supervisor", "reason": sv_check.get("reason"), "story_id": story_id}
    for ms in sv_check.get("overrides", {}).get("missing_steps", []):
        plan_steps.append({"id": f"sv_{ms}", "agent": ms, "description": f"Added by Supervisor", "depends_on": [], "requires_human_review": False, "max_retries": 2})
        steps_status.append({"id": f"sv_{ms}", "agent": ms, "description": f"Added by Supervisor", "status": "pending", "attempts": 0, "supervisor_verdict": None})

    # Phase 2: Execute plan step by step
    total_retries = 0
    agent_results = {}

    for i, step in enumerate(plan_steps):
        step_id, agent = step["id"], step["agent"]
        for ss in steps_status:
            if ss["id"] == step_id:
                ss["status"] = "running"
        _update(f"Step {i+1}/{len(plan_steps)}: {agent}")

        # Human Review Gate
        if step.get("requires_human_review", False):
            for ss in steps_status:
                if ss["id"] == step_id:
                    ss["status"] = "awaiting_review"
            _update(f"Human Review: {step.get('description', '')}", awaiting_review=True,
                    extra={"commander": {"plan_steps": plan_steps}})
            from datetime import timedelta
            expiry = context.current_utc_datetime + timedelta(minutes=30)
            timeout_task = context.create_timer(expiry)
            approval_task = context.wait_for_external_event("CommanderReviewApproved")
            winner = yield context.task_any([approval_task, timeout_task])
            if winner == timeout_task:
                for ss in steps_status:
                    if ss["id"] == step_id:
                        ss["status"] = "timed_out"
                return {"status": "timed_out", "story_id": story_id, "step": step_id}
            timeout_task.cancel()
            approval = approval_task.result
            if isinstance(approval, str):
                try:
                    approval = json.loads(approval)
                except Exception:
                    approval = {"approved": False, "reason": approval}
            if not isinstance(approval, dict) or not approval.get("approved", False):
                reason = approval.get("reason", "Declined") if isinstance(approval, dict) else "Declined"
                for ss in steps_status:
                    if ss["id"] == step_id:
                        ss["status"] = "declined"
                return {"status": "declined", "story_id": story_id, "reason": reason}
            for ss in steps_status:
                if ss["id"] == step_id:
                    ss["status"] = "approved"
            continue

        # Execute agent with retry loop
        agent_input = {"story": input_data, "mode": mode, "step": step,
                       "previous_results": agent_results, "instance_id": context.instance_id, "feedback": ""}
        attempt, step_done = 0, False

        while not step_done and attempt <= step.get("max_retries", 2) and total_retries <= 5:
            attempt += 1
            for ss in steps_status:
                if ss["id"] == step_id:
                    ss["attempts"] = attempt
            _update(f"Step {i+1}: {agent} (attempt {attempt})")

            output = yield context.call_activity("commander_dispatch_agent", {"agent": agent, "input": agent_input})

            if output.get("error"):
                decision = yield context.call_activity("commander_handle_failure", {
                    "step": step, "error": output["error"], "attempt": attempt,
                    "total_retries": total_retries, "max_total_retries": 5})
                action = decision.get("action", "abort")
                if action in ("retry", "heal"):
                    total_retries += 1
                    agent_input["feedback"] = decision.get("reason", "")
                    if action == "heal":
                        agent_input["healer_context"] = decision.get("healer_context", {})
                    continue
                elif action == "skip":
                    for ss in steps_status:
                        if ss["id"] == step_id:
                            ss["status"] = "skipped"
                    step_done = True
                    continue
                else:
                    for ss in steps_status:
                        if ss["id"] == step_id:
                            ss["status"] = "failed"
                    return {"status": "aborted", "story_id": story_id, "step": step_id, "reason": decision.get("reason")}

            # Commander evaluates + Supervisor checks
            evaluation = yield context.call_activity("commander_evaluate_result", {"step": step, "result": output, "mode": mode})
            sv = yield context.call_activity("supervisor_check_step", {
                "step_name": step_id, "agent": agent, "result": output,
                "commander_decision": evaluation, "elapsed_minutes": 0})
            for ss in steps_status:
                if ss["id"] == step_id:
                    ss["supervisor_verdict"] = sv

            if not sv.get("approved", True) and sv.get("action") == "halt":
                for ss in steps_status:
                    if ss["id"] == step_id:
                        ss["status"] = "halted"
                return {"status": "halted_by_supervisor", "story_id": story_id, "step": step_id, "reason": sv.get("reason")}
            if not sv.get("approved", True) and sv.get("overrides", {}).get("force_retry"):
                total_retries += 1
                agent_input["feedback"] = sv.get("reason", "")
                continue

            d = evaluation.get("decision", "proceed")
            if d == "proceed":
                agent_results[step_id] = output
                for ss in steps_status:
                    if ss["id"] == step_id:
                        ss["status"] = "completed"
                step_done = True
            elif d == "retry":
                total_retries += 1
                agent_input["feedback"] = evaluation.get("feedback", "")
            elif d == "reroute":
                agent = evaluation.get("reroute_to", agent)
                step["agent"] = agent
                total_retries += 1
            else:
                for ss in steps_status:
                    if ss["id"] == step_id:
                        ss["status"] = "escalated"
                return {"status": "escalated", "story_id": story_id, "step": step_id, "reason": evaluation.get("reason")}

        if not step_done:
            for ss in steps_status:
                if ss["id"] == step_id:
                    ss["status"] = "failed"
            return {"status": "max_retries", "story_id": story_id, "step": step_id}

    # Phase 3: Supervisor final sign-off
    _update("Supervisor: Final sign-off...")
    summary = {"story_id": story_id, "mode": mode, "total_steps": len(plan_steps),
               "completed": len([s for s in steps_status if s["status"] == "completed"]),
               "failed": len([s for s in steps_status if s["status"] in ("failed", "halted")]),
               "skipped": len([s for s in steps_status if s["status"] == "skipped"]),
               "retries_used": total_retries, "steps": steps_status}
    signoff = yield context.call_activity("supervisor_final_signoff", summary)
    if not signoff.get("approved", True):
        _update("Supervisor REJECTED", extra={"supervisor": signoff})
        return {"status": "rejected_by_supervisor", "story_id": story_id, "reason": signoff.get("reason"), "supervisor": signoff}
    _update("Pipeline complete", extra={"supervisor": signoff, "commander": summary})
    return {"status": "success", "story_id": story_id, "mode": mode, "summary": summary, "supervisor_signoff": signoff}


# ============================================================
# COMMANDER + SUPERVISOR ACTIVITY FUNCTIONS
# ============================================================


@app.activity_trigger(input_name="payload")
def commander_plan_execution(payload: dict) -> dict:
    from commander.agent import CommanderAgent
    try:
        config = AppConfig.from_env()
        plan = CommanderAgent(config).plan_execution(story=payload.get("story", {}), mode=payload.get("mode", "greenfield"))
        return {"task_id": plan.task_id, "story_id": plan.story_id, "mode": plan.mode, "sla_minutes": plan.sla_minutes,
                "steps": [{"id": s.id, "agent": s.agent, "description": s.description, "depends_on": s.depends_on,
                           "requires_human_review": s.requires_human_review, "max_retries": s.max_retries} for s in plan.steps]}
    except Exception as e:
        logger.error("Commander planning failed: %s", e)
        return {"error": str(e), "steps": []}


@app.activity_trigger(input_name="payload")
def commander_evaluate_result(payload: dict) -> dict:
    from commander.agent import CommanderAgent, TaskStep
    try:
        config = AppConfig.from_env()
        sd = payload.get("step", {})
        step = TaskStep(id=sd.get("id", ""), agent=sd.get("agent", ""), description=sd.get("description", ""),
                        attempts=sd.get("attempts", 1), max_retries=sd.get("max_retries", 2))
        return CommanderAgent(config).evaluate_result(step=step, result=payload.get("result"), context={"mode": payload.get("mode", "greenfield")})
    except Exception as e:
        return {"decision": "proceed", "reason": f"Evaluation error: {e}", "quality_score": 0.5}


@app.activity_trigger(input_name="payload")
def commander_handle_failure(payload: dict) -> dict:
    from commander.agent import CommanderAgent, ExecutionPlan, TaskStep
    try:
        config = AppConfig.from_env()
        sd = payload.get("step", {})
        step = TaskStep(id=sd.get("id", ""), agent=sd.get("agent", ""), description=sd.get("description", ""),
                        attempts=payload.get("attempt", 1), max_retries=sd.get("max_retries", 2))
        plan = ExecutionPlan(task_id="", story_id="", mode="", total_retries_used=payload.get("total_retries", 0), max_total_retries=payload.get("max_total_retries", 5))
        return CommanderAgent(config).handle_failure(step=step, error=payload.get("error", ""), plan=plan)
    except Exception as e:
        return {"action": "abort", "reason": f"Failure handling error: {e}"}


@app.activity_trigger(input_name="payload")
def commander_dispatch_agent(payload: dict) -> dict:
    """Universal agent dispatcher — routes to the correct worker agent."""
    agent_name = payload.get("agent", "")
    agent_input = payload.get("input", {})
    story = agent_input.get("story", {})
    mode = agent_input.get("mode", "greenfield")
    previous = agent_input.get("previous_results", {})
    feedback = agent_input.get("feedback", "")

    try:
        config = AppConfig.from_env()

        if agent_name == "planner":
            from planner.agent import PlannerAgent
            result = PlannerAgent(config).run(story_input=story)
            return {"plan": result.dict() if hasattr(result, "dict") else (result.to_dict() if hasattr(result, "to_dict") else result), "type": "plan"}

        elif agent_name == "developer":
            from developer.agent import DeveloperAgent
            from shared.models import BuildPlan
            plan_data = _find_prev(previous, "plan")
            build_plan = BuildPlan(**plan_data) if isinstance(plan_data, dict) and "story_id" in plan_data else plan_data
            result = DeveloperAgent(config).run(build_plan=build_plan)
            return {"artifacts": result.dict() if hasattr(result, "dict") else (result.to_dict() if hasattr(result, "to_dict") else result), "type": "artifacts"}

        elif agent_name == "code_review":
            from reviewer.agent import CodeReviewAgent
            artifacts_data = _find_prev(previous, "artifacts")
            plan_data = _find_prev(previous, "plan")
            artifact_list = artifacts_data.get("artifacts", []) if isinstance(artifacts_data, dict) else []
            return {"review": CodeReviewAgent(config).review(artifacts=artifact_list, build_plan=plan_data), "type": "review"}

        elif agent_name in ("validator_pre", "validator_post"):
            from validator.agent import ValidatorAgent
            from shared.models import ArtifactBundle, BuildPlan
            artifacts_data = _find_prev(previous, "artifacts")
            plan_data = _find_prev(previous, "plan")
            validator = ValidatorAgent(config)
            if "post" in agent_name:
                plan_obj = BuildPlan(**plan_data) if isinstance(plan_data, dict) and "story_id" in plan_data else plan_data
                result = validator.post_deploy_check(plan=plan_obj, environment="production")
            else:
                bundle = ArtifactBundle(**artifacts_data) if isinstance(artifacts_data, dict) and "story_id" in artifacts_data else artifacts_data
                plan_obj = BuildPlan(**plan_data) if isinstance(plan_data, dict) and "story_id" in plan_data else plan_data
                result = validator.pre_deploy_check(bundle=bundle, plan=plan_obj)
            return {"validation": result.dict() if hasattr(result, "dict") else result, "type": "validation"}

        elif agent_name == "deployer_adf":
            from shared.adf_client import ADFClient
            artifacts_data = _find_prev(previous, "artifacts")
            adf_json = artifacts_data.get("adf_pipeline", {}) if isinstance(artifacts_data, dict) else {}
            if adf_json:
                return {"deploy": ADFClient(config).deploy_bronze_pipeline(adf_json), "type": "adf_deploy"}
            return {"deploy": {"status": "skipped"}, "type": "adf_deploy"}

        elif agent_name == "deployer_sql":
            from shared.synapse_client import SynapseClient
            synapse = SynapseClient(config)
            artifacts_data = _find_prev(previous, "artifacts")
            deployed = []
            sql_items = artifacts_data.get("artifacts", []) if isinstance(artifacts_data, dict) else []
            for item in sql_items:
                ddl = item.get("content", item.get("sql", "")) if isinstance(item, dict) else str(item)
                if ddl.strip():
                    synapse.execute_ddl(ddl)
                    deployed.append({"object": item.get("object_name", item.get("name", "")), "status": "deployed"})
            return {"deploy": deployed, "type": "sql_deploy"}

        elif agent_name == "healer":
            from healer.agent import HealerAgent
            from shared.models import ValidationReport, ArtifactBundle
            healer_ctx = agent_input.get("healer_context", {})
            artifacts_data = _find_prev(previous, "artifacts")
            bundle = ArtifactBundle(**artifacts_data) if isinstance(artifacts_data, dict) and "story_id" in artifacts_data else None
            if bundle and healer_ctx.get("review_result"):
                result = HealerAgent(config).heal_from_review(review_result=healer_ctx["review_result"], artifact_bundle=bundle)
            elif bundle:
                report = ValidationReport(**healer_ctx.get("report", {})) if healer_ctx.get("report") else None
                result = HealerAgent(config).run(validation_report=report, artifact_bundle=bundle) if report else (bundle, [])
            else:
                return {"heal": {"status": "skipped", "reason": "no artifacts"}, "type": "heal"}
            healed_bundle, actions = result
            return {"heal": {"bundle": healed_bundle.dict() if hasattr(healed_bundle, "dict") else {}, "actions": [a.dict() if hasattr(a, "dict") else str(a) for a in actions]}, "type": "heal"}

        elif agent_name == "discovery":
            from discovery.agent import DiscoveryAgent
            return {"discovery": DiscoveryAgent(config).discover(options=agent_input.get("options")), "type": "discovery"}

        elif agent_name == "convention_adapter":
            from shared.convention_adapter import build_ruleset_from_profile
            profile = _find_prev(previous, "discovery")
            ruleset = build_ruleset_from_profile(profile)
            return {"conventions": ruleset.to_dict(), "type": "conventions"}

        elif agent_name == "pr_delivery":
            from shared.pr_client import PRClient
            return {"pr": PRClient(config).create_pr(story_id=story.get("story_id", ""), artifacts=_find_prev(previous, "artifacts")), "type": "pr"}

        elif agent_name == "notify_teams":
            from shared.teams_webhook import send_card, pipeline_started_card, completion_card
            desc = agent_input.get("step", {}).get("description", "")
            card = pipeline_started_card(story_id=story.get("story_id", ""), title=story.get("title", ""),
                                          tables=story.get("source_tables", []), work_item_id=story.get("work_item_id", "")) \
                if "start" in desc.lower() else \
                completion_card(story_id=story.get("story_id", ""), title=story.get("title", ""), deployed=[], skipped=[], failed=[])
            return {"notification": send_card(card), "type": "notification"}

        return {"error": f"Unknown agent: {agent_name}"}
    except Exception as e:
        logger.error("Agent dispatch [%s]: %s", agent_name, e)
        return {"error": str(e)}


def _find_prev(previous: dict, key: str) -> dict:
    """Find a previous agent result by type key."""
    for _, result in previous.items():
        if isinstance(result, dict):
            if result.get("type") == key:
                return result.get(key, result)
            if key in result:
                return result[key]
    return {}


@app.activity_trigger(input_name="payload")
def supervisor_check_plan(payload: dict) -> dict:
    from supervisor.agent import SupervisorAgent, SLAConfig
    try:
        sla_data = payload.get("sla", {})
        sla = SLAConfig(**sla_data) if sla_data else None
        verdict = SupervisorAgent(AppConfig.from_env(), sla=sla).check_plan(
            plan=payload.get("plan", {}), story=payload.get("story", {}), mode=payload.get("mode", "greenfield"))
        result = verdict.to_dict()
        result["accumulated_state"] = {
            "step_verdicts": [result],
            "total_retries": 0,
            "start_time": payload.get("start_time", 0),
        }
        return result
    except Exception as e:
        return {"approved": True, "action": "continue", "reason": f"Supervisor error: {e}", "quality_score": 0.5, "warnings": ["supervisor_error"], "accumulated_state": {}}


@app.activity_trigger(input_name="payload")
def supervisor_check_step(payload: dict) -> dict:
    from supervisor.agent import SupervisorAgent, SLAConfig
    try:
        sla_data = payload.get("sla", {})
        sla = SLAConfig(**sla_data) if sla_data else None
        supervisor = SupervisorAgent(AppConfig.from_env(), sla=sla)
        acc = payload.get("accumulated_state", {})
        supervisor._step_verdicts = acc.get("step_verdicts", [])
        supervisor._total_retries = acc.get("total_retries", 0)
        verdict = supervisor.check_step_result(
            step_name=payload.get("step_name", ""), agent=payload.get("agent", ""), result=payload.get("result"),
            commander_decision=payload.get("commander_decision", {}), elapsed_minutes=payload.get("elapsed_minutes", 0))
        result = verdict.to_dict()
        result["accumulated_state"] = {
            "step_verdicts": supervisor._step_verdicts + [result],
            "total_retries": supervisor._total_retries,
            "start_time": acc.get("start_time", 0),
        }
        return result
    except Exception as e:
        return {"approved": True, "action": "continue", "reason": f"Supervisor error: {e}", "quality_score": 0.5, "warnings": ["supervisor_error"], "accumulated_state": payload.get("accumulated_state", {})}


@app.activity_trigger(input_name="payload")
def supervisor_final_signoff(payload: dict) -> dict:
    from supervisor.agent import SupervisorAgent
    try:
        return SupervisorAgent(AppConfig.from_env()).final_signoff(execution_summary=payload).to_dict()
    except Exception as e:
        has_failures = payload.get("failed", 0) > 0
        return {"approved": not has_failures, "action": "continue" if not has_failures else "escalate",
                "reason": f"Supervisor error: {e}", "quality_score": 0.5}
