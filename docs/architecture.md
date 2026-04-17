# BI Automation Platform - Architecture Document

**Version**: 7.0
**Last Updated**: 2026-04-15

---

## 1. Platform Overview

**Two independent products** in one repository, sharing Azure infrastructure but deployed and scaled separately:

| Product | Directory | Function App | Purpose |
|---------|-----------|-------------|---------|
| **BI Pipeline Automation** | `agents/` | `{prefix}-{env}-func` | Reads stories, generates medallion SQL, deploys to Synapse |
| **Test Automation Platform** | `test-automation/` | `{prefix}-{env}-test-func` | AI-powered UI + Data testing for Power Apps |

### Product 1: BI Pipeline Automation (9 AI Agents)
- **Commander + Supervisor architecture**: Commander decomposes tasks, dispatches agents dynamically; Supervisor independently validates quality
- Story-to-deployed-data-model in minutes (not weeks)
- Greenfield, brownfield, and Integration Mode detection
- AI code review with self-healing before deployment
- Human review gate for approval before deploy
- Self-correcting loop: Bug Fixer Agent reads ADO bugs, generates fix, re-tests
- Real-time pipeline visualization (Web UI + Teams)
- Ops Module: auto-pause Synapse, secret alerts, prompt regression, DB cleanup

### Product 2: Test Automation Platform (6 AI Agents)
- AI routes stories to UI, Data, or Both test paths
- Playwright scripts generated and run in headed Edge (QA watches in real-time)
- SQL data validation tests executed server-side
- Auto-creates ADO Test Plans, Cases, Runs, and Bug work items
- Excel reports + Teams adaptive cards

### Integration Mode (NEW in v4.0)
The platform can plug into an **existing** client data platform instead of deploying from scratch:

| Component | Purpose |
|-----------|---------|
| **Discovery Agent** | Scans client's existing Synapse schemas, ADF pipelines, ADLS folders, Power BI workspace |
| **Convention Adapter** | Auto-detects naming patterns, schema purposes, distribution strategies |
| **PR Delivery Mode** | Delivers artifacts as a Pull Request to the client's repo (not direct deploy) |

```
GREENFIELD/BROWNFIELD (current):    INTEGRATION MODE (new):
  Story -> Build -> Deploy             Story -> Discover -> Build matching their style -> PR to their repo
  (we own the stack)                   (we augment their stack)
```

---

## 2. High-Level Architecture

```
                            +------------------+
                            |   Azure DevOps   |
                            | (Stories + Code)  |
                            +--------+---------+
                                     |
                     Story fetch / artifact commit / test results
                                     |
                            +--------v---------+
                            |   Web UI (SPA)   |
                            |  15 pages, login |
                            +---+----------+---+
                                |          |
                           BI API      Test API
                                |          |
     +--------------------------v--+  +----v--------------------------+
     | PRODUCT 1: BI Pipeline      |  | PRODUCT 2: Test Automation    |
     | {prefix}-{env}-func         |  | {prefix}-{env}-test-func      |
     |                              |  |                               |
     | 44 HTTP Endpoints            |  | 11 HTTP Endpoints             |
     | 2 Orchestrators + 3 Timers   |  | 1 Orchestrator (6 steps)      |
     |                              |  |                               |
     | +--------+ +----------+     |  | +----------+ +-----------+    |
     | |Planner | |Developer |     |  | |Test      | |Data Test  |    |
     | | Agent  | | Agent    |     |  | | Router   | | Planner   |    |
     | +--------+ +----------+     |  | +----------+ +-----------+    |
     | +--------+ +----------+     |  | +----------+ +-----------+    |
     | |Code    | |Validator |     |  | |Test      | |Data Test  |    |
     | |Review  | | Agent    |     |  | | Planner  | | Executor  |    |
     | +--------+ +----------+     |  | +----------+ +-----------+    |
     | +--------+ +----------+     |  | +----------+ +-----------+    |
     | |Healer  | |Discovery |     |  | |Test      | |Test       |    |
     | | Agent  | | Agent    |     |  | |Generator | | Reporter  |    |
     | +--------+ +----------+     |  | +----------+ +-----------+    |
     | +--------+                  |  |                               |
     | |Bug     |                  |  |  Bug Found? ──────────────┐   |
     | |Fixer   |<─── fix loop ───|──|──────────────────────────  │   |
     | +--------+                  |  |                               |
     |                              |  |                               |
     | ┌──── OPS MODULE ────────┐  |  |                               |
     | │ Auto-pause (30m timer) │  |  |                               |
     | │ Secret alerts (daily)  │  |  |                               |
     | │ DB cleanup (weekly)    │  |  |                               |
     | │ Regression tests       │  |  |                               |
     | │ Ops dashboard          │  |  |                               |
     | └────────────────────────┘  |  |                               |
     +-----+--------+--------+----+  +----+--------+--------+--------+
           |        |        |             |        |        |
           v        v        v             v        v        v
     +---------+ +------+ +--------+  +--------+ +------+ +--------+
     | Synapse | | Key  | | Data   |  | Azure  | | Key  | | Azure  |
     | Dedic.  | | Vault| |Factory |  | SQL DB | | Vault| | AI     |
     | Pool    | |      | |        |  |(Source) | |      | |Foundry |
     +---------+ +------+ +--------+  +--------+ +------+ +--------+
                                                      |
                                              +-------v--------+
                                              | Local Agent    |
                                              | (QA machine)   |
                                              | Playwright Edge|
                                              +----------------+

     Shared: App Service Plan (EP1), Storage Account, App Insights
```

### Integration Mode Architecture

```
  ┌──────────────────────────────────────────────────────────────────────┐
  │                 CLIENT'S EXISTING PLATFORM                          │
  │                                                                     │
  │  ┌─────────────┐  ┌─────────────┐  ┌─────────┐  ┌──────────────┐  │
  │  │ Synapse     │  │ Data        │  │ ADLS    │  │ Power BI     │  │
  │  │ 500+ tables │  │ Factory     │  │ Gen2    │  │ Workspace    │  │
  │  │ 20 schemas  │  │ 200+ pipes  │  │ 50 TB   │  │ 100+ reports │  │
  │  └──────┬──────┘  └──────┬──────┘  └────┬────┘  └──────┬───────┘  │
  │         │                │              │               │          │
  └─────────┼────────────────┼──────────────┼───────────────┼──────────┘
            │                │              │               │
            v                v              v               v
  ┌─────────────────────────────────────────────────────────────────────┐
  │                    DISCOVERY AGENT                                  │
  │  Scans all objects, columns, distributions, naming patterns, etc.  │
  └────────────────────────────┬────────────────────────────────────────┘
                               │
                               v  EnvironmentProfile
  ┌─────────────────────────────────────────────────────────────────────┐
  │                    CONVENTION ADAPTER                               │
  │  Auto-detects: naming case, prefixes, schema purposes, layer map   │
  │  Generates: ConventionRuleset injected into all agent LLM prompts  │
  └────────────────────────────┬────────────────────────────────────────┘
                               │
                               v  ConventionRuleset
  ┌─────────────────────────────────────────────────────────────────────┐
  │  STANDARD PIPELINE (convention-aware)                               │
  │  Planner -> Developer -> Code Review -> Validator                  │
  │  (all agents receive ConventionRuleset as prompt context)           │
  └────────────────────────────┬────────────────────────────────────────┘
                               │
                               v  Generated Artifacts
  ┌─────────────────────────────────────────────────────────────────────┐
  │                    PR DELIVERY MODE                                 │
  │  Creates branch: feature/auto-{story_id}                           │
  │  Pushes: SQL + ADF JSON + README with checklist                    │
  │  Opens: Pull Request in client's ADO repo                          │
  │  Client reviews + their CI/CD deploys                              │
  └─────────────────────────────────────────────────────────────────────┘
```

---

## 2.1 Commander + Supervisor Architecture (NEW in v7.0)

The platform uses a **two-tier orchestration** model that replaces the hardcoded agent chain with dynamic, LLM-driven task decomposition and an independent quality watchdog.

### Commander Agent

The Commander receives a story and uses the LLM to create a dynamic execution plan — deciding which agents to call, in what order, and whether to parallelize.

```
Story Input
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│                   COMMANDER AGENT                        │
│                                                          │
│  1. Analyze story with LLM                               │
│  2. Decompose into sub-tasks                             │
│  3. Select agents + ordering                             │
│  4. Dispatch agents (sequential or parallel)             │
│  5. Evaluate each result (quality score)                 │
│  6. On failure: reason → retry / reroute / escalate      │
│  7. Return consolidated result                           │
└─────────────────────────────────────────────────────────┘
```

**Key capabilities:**
- Dynamic agent dispatch (not hardcoded)
- Result evaluation with quality scoring
- Failure recovery with reasoning (retry with feedback, reroute to different agent, or escalate)
- SLA tracking (max duration, max retries)

### Supervisor Agent

An **independent** quality watchdog that monitors the Commander. The Supervisor is NOT in the Commander's chain of command — it validates independently.

```
┌─────────────────────────────────────────────────────────┐
│                   SUPERVISOR AGENT                       │
│                                                          │
│  ✓ Validates Commander's execution plan                  │
│  ✓ Checks: all required steps present?                   │
│  ✓ Checks: ordering logical? security gates exist?       │
│  ✓ Monitors SLA (duration, retry counts)                 │
│  ✓ Performs final sign-off before reporting success       │
│  ✓ Can override Commander decisions                      │
└─────────────────────────────────────────────────────────┘
```

### Commander Orchestrator (Durable Functions)

Replaces the previous hardcoded orchestration chain with a dynamic Commander-driven flow:

```
commander_orchestrator (Durable Functions)
    │
    ├── activity: commander_analyze      (LLM decomposes story)
    ├── activity: supervisor_validate    (independent plan check)
    ├── activity: commander_dispatch     (calls worker agents per plan)
    │       ├── planner_agent
    │       ├── human_review_gate
    │       ├── developer_agent
    │       ├── code_review_agent
    │       ├── deploy_activities
    │       └── validator_agent
    ├── activity: commander_evaluate     (quality check each result)
    ├── activity: commander_heal         (on failure: retry/reroute)
    └── activity: supervisor_signoff     (final independent review)
```

### Commander Endpoints (4)

| Method | Route | Auth | Purpose |
|--------|-------|------|---------|
| POST | `/api/commander/run` | Function Key | Start Commander-driven pipeline |
| GET | `/api/commander/status` | Function Key | Commander orchestration status |
| POST | `/api/commander/approve` | Function Key | Approve Commander plan |
| POST | `/api/commander/decline` | Function Key | Decline Commander plan |

---

## 3. Azure Resource Inventory

| Resource | Name Pattern | SKU/Tier | Purpose |
|----------|-------------|----------|---------|
| **Storage Account** (ADLS Gen2) | `{prefix}{env}{suffix}` | Standard_GRS | Bronze data lake, agent artifacts |
| **Key Vault** | `{prefix}-{env}-kv` | Standard | All secrets (SQL password, AI key, ADO PAT, Teams webhook) |
| **Synapse Workspace** | `{prefix}-{env}-syn` | Standard | Medallion data warehouse |
| **Synapse Dedicated Pool** | `bipool` | DW100c | SQL compute for Bronze/Silver/Gold |
| **Azure SQL Database** | `{prefix}-{env}-sourcedb` | Basic (5 DTU) | Source data + Config DB + Metadata Catalog |
| **Azure SQL Server** | `{prefix}-{env}-sqlsrv` | N/A | Host for Azure SQL DB |
| **Function App (BI)** | `{prefix}-{env}-func` | Premium EP1 | BI pipeline (31 endpoints, 1 orchestrator, 20 activities) |
| **Function App (Test)** | `{prefix}-{env}-test-func` | Shared EP1 | Test automation (11 endpoints, 1 orchestrator, 9 activities) |
| **App Service Plan** | `{prefix}-{env}-asp` | ElasticPremium EP1 | Shared compute for both Function Apps (max 3 workers) |
| **Azure Data Factory** | `{prefix}-{env}-adf` | Standard | Incremental load pipelines, daily triggers |
| **Application Insights** | `{prefix}-{env}-ai` | Standard | Telemetry, logging, performance monitoring |
| **Log Analytics** | `{prefix}-{env}-log` | PerGB2018 | Centralized log storage (90-day retention) |
| **Azure OpenAI** *(optional)* | `{prefix}-{env}-oai` | S0 | GPT-4o model (deployed only if `deployOpenAi=true`) |

### Security Configuration

| Resource | Network | Auth | Encryption |
|----------|---------|------|------------|
| Storage | Deny by default, AzureServices bypass | Managed identity (no shared keys) | GRS, no public blob access |
| Key Vault | Deny by default, AzureServices bypass | RBAC (Secrets User role) | Purge protection, 90-day soft-delete |
| Synapse | Azure services only | SQL auth + AAD admin | TLS 1.2 |
| SQL Database | Azure services only | SQL auth | TLS 1.2 |
| Function App (BI) | HTTPS only, FTPS disabled | Function keys + Key Vault refs | TLS 1.2 |
| Function App (Test) | HTTPS only, FTPS disabled | Function keys + Key Vault refs | TLS 1.2 |

---

## 4. BI Pipeline: Commander-Driven Orchestration

```
 [1] Commander Analyzes Story
           │
 [2] Supervisor Validates Plan ──── (can override/add steps)
           │
 [3] Commander Dispatches Planner Agent
           │
 [4] Human Review Gate (Commander pauses, Supervisor monitors)
           │
 [5] Commander Dispatches Developer Agent ──── (evaluates result quality)
           │                                         │
           │                              (low quality? → feedback loop)
           │
 [6] Commander Dispatches Code Review ──── (non-compliant? → reroute to Developer)
           │
 [7] Commander Deploys ADF + SQL ──── (failure? → reason: retry/heal/escalate)
           │
 [8] Commander Dispatches Validator + Healer ──── (self-healing, max 3 retries)
           │
 [9] Supervisor Final Sign-off ──── (all steps OK? SLA met? → report success)
```

### Step Details

| Step | Agent | What Happens | Output |
|------|-------|-------------|--------|
| **1. Fetch Story** | ADO Client | Fetches work item from Azure DevOps by ID | Title, description, acceptance criteria |
| **2. Extract Tables** | Story Mapper + Universal Interpreter | AI detects format (Gherkin/English/technical), extracts source tables | List of `schema.table` names |
| **3. Planner Agent** | PlannerAgent (AI) | Analyzes requirements, detects greenfield/brownfield, generates build plan with execution order | BuildPlan (mode, risk, steps, validations) |
| **Human Review Gate** | - | Pipeline pauses. User sees plan in Web UI + Teams adaptive card. Must approve or decline. 30-min timeout. | Approved / Declined |
| **4. Developer Agent** | DeveloperAgent (AI) | Generates SQL DDL, views, stored procs, ADF pipeline JSON from build plan. Uses templates + catalog metadata to prevent hallucination. | ArtifactBundle (SQL + ADF JSON files) |
| **5. Code Review** | CodeReviewAgent (AI) + HealerAgent | AI reviews all artifacts for 7 categories (security, performance, Synapse compat, naming, idempotency, business logic, ADF). On REJECT/NEEDS_FIX: Healer auto-fixes and re-submits (up to 3 retries). | APPROVE / REJECT with findings |
| **6. Deploy ADF** | ADFClient | Deploys pipeline JSON, datasets, and daily trigger to Azure Data Factory | Pipeline + datasets + trigger deployed |
| **7. Pre-Validation** | ValidatorAgent + HealerAgent | Static checks: schema naming, SQL syntax, Synapse compatibility, naming conventions. On failure: Healer auto-fixes (up to 3 retries). | ValidationReport (pass/fail per check) |
| **8. Deploy to Synapse** | SynapseClient | Executes DDL on Synapse Dedicated Pool. Brownfield mode skips existing objects. | Deployed/Skipped/Failed per object |
| **9. Post-Validation** | ValidatorAgent | Data quality checks: row counts, null ratios, duplicate detection, referential integrity | ValidationReport + DQ scores |

### Notifications

Teams adaptive cards sent at:
- Pipeline started (story ID, tables, instance link)
- Review gate (plan details, approve/decline action buttons)
- Pipeline completed (success/failure summary, artifact counts)

---

## 5. Test Automation: 6-Step Orchestration

```
 [1] Read Story --> [2] AI Router --> [3] Plan Tests --> [4] ADO Artifacts
                        |                                      |
                    UI / Data / Both                            |
                        |                                      v
                        +---> [5a] UI: Generate Playwright --> Local Agent (headed Edge)
                        |         scripts, queue job,          |
                        |         wait for results             v
                        |                                QA watches browser
                        |
                        +---> [5b] Data: Execute SQL     --> Server-side execution
                        |         against Synapse              |
                        |                                      v
                        +---> [5c] Both: Parallel        SQL results + UI results
                                                               |
                                                               v
                                                   [6] Report to ADO + Teams
```

### AI Test Router

The TestRouter agent classifies each story as:
- **UI** - Power Apps Canvas testing (Playwright in headed Edge)
- **Data** - SQL validation tests against Synapse/SQL
- **Both** - Parallel execution of UI + Data tests

### Local Agent Package (`bi-test-agent`)

Python package installed on QA's machine. QA watches AI drive the browser in real-time.

```
pip install -e agents/tester/local-agent
bi-test-agent connect --server https://{func-app}.azurewebsites.net --key {func-key}
bi-test-agent run --server ... --key ... --run-id {run-id}
```

**Flow**: Agent polls `/api/agent-poll` -> downloads test scripts -> launches Edge in headed mode -> executes Playwright tests with live log streaming to `/api/agent-log` -> uploads results to `/api/agent-results` -> generates Excel report

---

## 6. AI Agents (15 Total, Split Across Products)

### Command Layer (NEW in v7.0) — `agents/`

| Agent | Purpose | Input | Output | LLM Model |
|-------|---------|-------|--------|-----------|
| **CommanderAgent** | Task decomposition, dynamic agent dispatch, result evaluation, failure recovery | Story / task | Dynamic execution plan + consolidated result | Phi-4 / GPT-4o |
| **SupervisorAgent** | Independent quality watchdog, SLA enforcement, plan validation, final sign-off | Commander plan / results | Plan verdict + overrides + final sign-off | Phi-4 / GPT-4o |

### Product 1: BI Pipeline Worker Agents (7) — `agents/`

| Agent | Purpose | Input | Output | LLM Model |
|-------|---------|-------|--------|-----------|
| **PlannerAgent** | Analyze story, detect mode, generate build plan | StoryContract | BuildPlan | Phi-4 / GPT-4o |
| **DeveloperAgent** | Generate SQL DDL + ADF pipeline JSON | BuildPlan | ArtifactBundle | Phi-4 / GPT-4o |
| **CodeReviewAgent** | Review generated code for 7 quality categories | Artifacts + Plan | Review verdict + findings | Phi-4 / GPT-4o |
| **ValidatorAgent** | Pre-deploy syntax + post-deploy data quality checks | ArtifactBundle / Plan | ValidationReport | Rule-based + SQL |
| **HealerAgent** | Auto-fix validation/review failures, escalate if unable | Report + Bundle | Corrected Bundle + Actions | Phi-4 / GPT-4o |
| **DiscoveryAgent** | Scan existing Synapse/ADF/ADLS, build environment profile | Connection details | EnvironmentProfile + ConventionRuleset | Rule-based (no LLM) |
| **BugFixerAgent** | Read ADO bug, analyze root cause, generate corrected code | Bug details + original artifacts | Corrected artifacts + root cause + recommendation | Phi-4 / GPT-4o |

### Product 2: Test Agents (6) — `test-automation/tester/`

| Agent | Purpose | Input | Output | LLM Model |
|-------|---------|-------|--------|-----------|
| **TestRouter** | Classify story as UI/Data/Both | Story payload | test_type + confidence | Phi-4 / GPT-4o |
| **TestPlannerAgent** | Generate UI test scenarios | Story + app URL | Test scenarios with steps | Phi-4 / GPT-4o |
| **DataTestPlanner** | Generate SQL validation tests with custom categories | Story + data aspects | SQL test queries | Phi-4 / GPT-4o |
| **TestGeneratorAgent** | Generate Playwright Python scripts | Test plan | .py test files + page objects | Phi-4 / GPT-4o |
| **DataTestExecutor** | Execute SQL tests server-side | SQL queries | pass/fail per test | SQL execution |
| **TestReporter** | Report results to ADO + Teams + Excel | Execution results | Test Runs, Bugs, adaptive cards | ADO REST API |

### Code Review Categories (7)

| Category | What It Checks |
|----------|---------------|
| **Security** | SQL injection, exposed credentials, overly permissive grants |
| **Performance** | Cartesian joins, full table scans, SELECT *, missing WHERE |
| **Synapse Compatibility** | Unsupported types, missing distribution hints, missing CCI |
| **Naming Standards** | Schema prefix, snake_case, proc/view naming patterns |
| **Idempotency** | IF NOT EXISTS, CREATE OR ALTER, duplicate handling |
| **Business Logic** | SQL matches build plan, correct JOINs and aggregations |
| **ADF Pipeline** | Valid JSON, linked service refs, copy mappings |

---

## 7. Database Schema Architecture

### Azure SQL Database (Config DB + Source)

```
+---------------------+    +---------------------+    +---------------------+
|    catalog schema    |    |    config schema     |    |    audit schema      |
|---------------------|    |---------------------|    |---------------------|
| source_systems      |    | pipeline_registry   |    | agent_execution_log |
| source_tables       |    | execution_log       |    | validation_results  |
| approved_joins      |    | artifact_versions   |    | healer_actions      |
| business_glossary   |    | semantic_definitions|    +---------------------+
| naming_conventions  |    | feedback            |
| deployment_log      |    | source_connectors   |    +---------------------+
| layer_inventory     |    | column_lineage      |    |    dbo schema        |
+---------------------+    +---------------------+    |---------------------|
                                                       | test_runs           |
                                                       | test_cases          |
                                                       | test_bugs           |
                                                       +---------------------+
```

### Synapse Dedicated Pool (Medallion)

```
+-------------------+    +-------------------+    +-------------------+
|   bronze schema   |    |   silver schema   |    |    gold schema    |
|-------------------|    |-------------------|    |-------------------|
| External tables   |    | Cleansed views    |    | Aggregated views  |
| (ADLS Parquet)    | -> | (dedup, type-cast)| -> | (business-ready)  |
| ext_sales_*       |    | vw_sales_*        |    | vw_customer_*     |
| sales_*           |    | sales_daily_*     |    | vw_sales_daily_*  |
+-------------------+    +-------------------+    +-------------------+
```

---

## 8. API Inventory

### PRODUCT 1: BI Pipeline — 48 Endpoints (`{prefix}-{env}-func`)

### Commander Operations (4) — NEW in v7.0

| Method | Route | Auth | Purpose |
|--------|-------|------|---------|
| POST | `/api/commander/run` | Function Key | Start Commander-driven pipeline (body: story payload) |
| GET | `/api/commander/status` | Function Key | Commander orchestration status + agent dispatch log |
| POST | `/api/commander/approve` | Function Key | Approve Commander's execution plan |
| POST | `/api/commander/decline` | Function Key | Decline Commander's plan with reason |

### Pipeline Operations (10)

| Method | Route | Auth | Purpose |
|--------|-------|------|---------|
| POST | `/api/process-story` | Function Key | Start pipeline from JSON story input |
| POST | `/api/process-ado-story` | Function Key | Fetch ADO work item + start pipeline |
| POST | `/api/process-free-story` | Function Key | Start pipeline from free-text (no ADO) |
| POST | `/api/preview-ado-story` | Function Key | Preview story extraction (no execution) |
| POST | `/api/approve-plan` | Function Key | Approve human review gate |
| POST | `/api/decline-plan` | Function Key | Decline with reason |
| POST | `/api/cancel-pipeline` | Function Key | Terminate running pipeline |
| GET | `/api/pipeline-status` | Function Key | Human-readable status summary |
| GET | `/api/story-status` | Function Key | Lookup by work_item_id |
| GET/POST | `/api/pipeline-progress` | Function Key | Step-by-step visual progress |

### Data Catalog & Query (6)

| Method | Route | Auth | Purpose |
|--------|-------|------|---------|
| GET | `/api/data-lineage` | Function Key | Live row counts per layer |
| GET | `/api/data-catalog` | Function Key | Browse all objects + columns |
| POST | `/api/nl-query` | Function Key | Natural language to SQL (sandboxed, gold/silver only) |
| GET | `/api/column-lineage` | Function Key | Column-level lineage graph |
| POST | `/api/data-quality` | Function Key | On-demand DQ checks |
| POST | `/api/generate-pbi` | Function Key | Generate Power BI dataset (TMSL JSON) |

### Admin & Config (9)

| Method | Route | Auth | Purpose |
|--------|-------|------|---------|
| GET | `/api/health` | Anonymous | Health check (Synapse, Config DB, LLM, ADF) |
| GET | `/api/ui` | Anonymous | Serve web UI with security headers |
| GET | `/api/pipeline-history` | Function Key | Execution history from Config DB |
| GET | `/api/artifact-history` | Function Key | Version history per artifact |
| GET | `/api/templates` | Function Key | Pipeline template library |
| POST | `/api/use-template` | Function Key | Create ADO work item from template |
| GET/POST | `/api/semantic` | Function Key | Semantic layer CRUD |
| GET/POST | `/api/feedback` | Function Key | Feedback CRUD |
| GET/POST/PUT/DELETE | `/api/connectors` | Function Key | Source connector CRUD |

### Schedules & Costs (3)

| Method | Route | Auth | Purpose |
|--------|-------|------|---------|
| GET | `/api/schedules` | Function Key | ADF pipeline schedules |
| GET | `/api/costs` | Function Key | Azure cost estimates |
| POST | `/api/notify` | Function Key | Send Teams notification |

### Teams Bot (3)

| Method | Route | Auth | Purpose |
|--------|-------|------|---------|
| POST | `/api/bot-message` | Anonymous | Bot Framework messaging endpoint |
| POST | `/api/bot-notify` | Function Key | Proactive Teams notification |
| POST | `/api/interpret-story` | Function Key | Universal story interpreter |

### Bug Fixer (3) — NEW in v5.0

| Method | Route | Auth | Purpose |
|--------|-------|------|---------|
| POST | `/api/fix-bug` | Function Key | Start bug fix orchestrator (body: {bug_id, auto_deploy?, re_test?}) |
| GET | `/api/fix-status/{id}` | Function Key | Check bug fix orchestrator progress |
| POST | `/api/fix-approve/{id}` | Function Key | Approve/decline a bug fix before deployment |

### Ops Module (7) — NEW in v6.0

| Method | Route | Auth | Purpose |
|--------|-------|------|---------|
| GET | `/api/ops/dashboard` | Function Key | Full operational dashboard (agent stats, secrets, idle, DB counts) |
| GET | `/api/ops/agent-stats` | Function Key | Per-agent failure rates, avg duration, success rate |
| GET | `/api/ops/secret-health` | Function Key | Credential validation (ADO PAT, AI key, SQL, Teams) |
| GET | `/api/ops/synapse-idle` | Function Key | Check if Synapse pool should be paused |
| POST | `/api/ops/pause-synapse` | Function Key | Manually trigger Synapse pool pause |
| POST | `/api/ops/regression-test` | Function Key | Run prompt regression tests (3 known stories) |
| POST | `/api/ops/cleanup` | Function Key | DB retention cleanup (body: {retention_days}) |

### Integration Mode (3) — NEW in v4.0

| Method | Route | Auth | Purpose |
|--------|-------|------|---------|
| POST | `/api/discover` | Function Key | Run Discovery Agent, scan existing platform, return EnvironmentProfile |
| GET/POST | `/api/conventions` | Function Key | GET: current ruleset. POST: generate ConventionRuleset from discovery profile |
| POST | `/api/deliver-pr` | Function Key | Create PR in client's ADO repo with generated artifacts |

---

### PRODUCT 2: Test Automation — 11 Endpoints (`{prefix}-{env}-test-func`)

| Method | Route | Auth | Purpose |
|--------|-------|------|---------|
| GET | `/api/health` | Anonymous | Health check (LLM connectivity) |
| POST | `/api/run-tests` | Function Key | Start test orchestration |
| GET | `/api/test-progress` | Function Key | Test pipeline progress |
| POST | `/api/ado-webhook` | Function Key | ADO webhook (auto-trigger on state change) |
| GET | `/api/download-tests` | Function Key | Download Playwright test ZIP |
| POST | `/api/upload-results` | Function Key | Upload JUnit XML results |
| POST/GET | `/api/agent-log` | Function Key | Local agent log streaming |
| GET | `/api/agent-poll` | Function Key | Local agent job polling |
| POST | `/api/queue-agent-job` | Function Key | Queue UI test job |
| POST | `/api/agent-results` | Function Key | Upload local agent results |
| GET | `/api/download-data-report` | Function Key | Download data test Excel report |
| GET/POST/PUT/DELETE | `/api/test-categories` | Function Key | Custom data test category CRUD |

---

## 9. Web UI Architecture (15 Pages)

```
+------------------------------------------------------------------+
|  Login Overlay (API key authentication)                          |
+------------------------------------------------------------------+
|  +----------+  +----------------------------------------------+  |
|  | Sidebar  |  | Content Area                                 |  |
|  |----------|  |----------------------------------------------|  |
|  | Pipeline |  | [Pipeline] 9-node visual flow + terminal log |  |
|  | Dashboard|  | [Dashboard] Pipeline history, stats          |  |
|  | Data     |  | [Data Lineage] Animated flow diagram         |  |
|  |  Lineage |  | [Catalog] Table/column browser + search      |  |
|  | Catalog  |  | [Query] Natural language to SQL + results    |  |
|  | Query    |  | [Templates] Pipeline template library        |  |
|  | Templates|  | [Semantic] Business term definitions CRUD    |  |
|  | Semantic |  | [Schedules] ADF pipeline schedules           |  |
|  | Schedules|  | [Costs] Azure cost dashboard                 |  |
|  | Costs    |  | [Feedback] Issue reporting + tracking        |  |
|  | Feedback |  | [Connectors] Multi-source connector manager  |  |
|  | Connectors| | [Testing] Test automation with progress      |  |
|  | Testing  |  | [Power BI] Dataset generation                |  |
|  | Power BI |  | [Notifications] Teams notification log       |  |
|  | Notifs   |  +----------------------------------------------+  |
|  +----------+                                                    |
+------------------------------------------------------------------+
```

### Pipeline Visual Flow (9 Nodes — Commander/Supervisor)

```
[Commander] -> [Supervisor Check] -> [Planner Agent] -> [Human Review]
                                                              |
                                                    (approve/decline)
                                                              |
[Developer Agent] -> [Code Review] -> [Deploy ADF+SQL] -> [Validate+Heal] -> [Supervisor Sign-off]
```

Each node shows: status (pending/running/completed/failed/escalated), detail text, elapsed time.
Clicking a node shows: expanded details (Commander plan, agent dispatch log, quality scores, Supervisor verdicts).

---

## 10. Shared Module Architecture (17 Modules)

| Module | File | Dependencies | Purpose |
|--------|------|-------------|---------|
| **AppConfig** | `shared/config.py` | - | Central config from env vars. Fail-fast on missing required vars. |
| **LLMClient** | `shared/llm_client.py` | OpenAI SDK | AI calls with retry, token tracking, JSON extraction fallbacks |
| **SynapseClient** | `shared/synapse_client.py` | pyodbc | DDL execution, parameterized queries, connection retry |
| **ADOClient** | `shared/ado_client.py` | requests | Work item fetch, comment posting |
| **ADFClient** | `shared/adf_client.py` | azure-mgmt-datafactory | Pipeline/dataset/trigger deployment |
| **StateRegistry** | `shared/state_registry.py` | pyodbc | Pipeline lifecycle, step logging, artifact versioning |
| **StoryMapper** | `shared/story_mapper.py` | LLMClient | ADO work item -> StoryContract (3 routing paths) |
| **StoryInterpreter** | `shared/story_interpreter.py` | LLMClient | Universal format: Gherkin, English, bullets, mixed |
| **ArtifactVersioner** | `shared/artifact_versioner.py` | requests | Git commit generated SQL to ADO repo |
| **ConnectorClient** | `shared/connector_client.py` | pyodbc, requests | Multi-source connector CRUD + data preview |
| **DataQuality** | `shared/data_quality.py` | SynapseClient | DQ checks: completeness, freshness, accuracy |
| **LineageTracker** | `shared/lineage_tracker.py` | pyodbc | Column-level lineage extraction + persistence |
| **TeamsBot** | `shared/teams_bot.py` | botbuilder | Bot Framework conversational commands |
| **TeamsWebhook** | `shared/teams_webhook.py` | requests | Adaptive card builder (4 card types) |
| **SharePointClient** | `shared/sharepoint_client.py` | requests | Graph API integration for test data |
| **OpsManager** | `shared/ops.py` | pyodbc, requests | Dashboard, auto-pause, secret health, regression, cleanup (454 lines) |
| **Models** | `shared/models.py` | pydantic | 16 Pydantic models (StoryContract, BuildPlan, etc.) |
| **Blueprints** | `blueprints/*.py` | azure.durable_functions | Modular endpoint organization (admin, catalog, pipeline) |

---

## 11. Pydantic Data Models

```
StoryContract
  +-- story_id, title, business_objective
  +-- source_tables[], dimensions[], metrics[]
  +-- joins[], acceptance_criteria[]

BuildPlan
  +-- story_id, mode (ExecutionMode), risk_level (RiskLevel)
  +-- execution_order: BuildStep[]
  |     +-- step, layer, action, artifact_type
  |     +-- object_name, source (SourceTarget)
  |     +-- logic_summary, load_pattern
  +-- validation_requirements: ValidationRequirement[]

ArtifactBundle
  +-- story_id
  +-- artifacts: GeneratedArtifact[]
        +-- step, artifact_type, object_name
        +-- layer, file_name, content (SQL/JSON)

ValidationReport
  +-- story_id, phase (pre_deploy/post_deploy)
  +-- overall_status (PASS/FAIL/WARN)
  +-- checks: ValidationCheck[]
  +-- blocking_failures[], warnings[]

PipelineState
  +-- story_id, mode, status
  +-- build_plan, artifacts, deploy_result
  +-- validation_report, healer_actions
  +-- retry_count, error_log
```

---

## 12. CI/CD Pipeline

### CI Pipeline (on PR)

```
+------------------+    +------------------+    +------------------+
| Lint + Tests     |    | Security Scan    |    | Validate SQL     |
|------------------|    |------------------|    |------------------|
| flake8 (Python)  |    | pip-audit (CVEs) |    | Static SQL checks|
| mypy (type check)|    | bandit (SAST)    |    |                  |
| pytest (126 tests)|   | Secret detection |    |                  |
| Coverage >= 60%  |    |                  |    |                  |
+------------------+    +------------------+    +------------------+

+------------------+
| Validate Bicep   |
|------------------|
| az bicep build   |
+------------------+
```

### CD Pipeline (on merge to develop)

```
[Deploy Infra] -> [Deploy Catalog DDL] -> [Deploy to Staging Slot]
      |                    |                         |
      v                    v                    [Warm Up]
  Bicep deploy       Run SQL scripts                 |
                                                [Smoke Test]
                                                     |
[Seed Source Data]                         [Swap Staging -> Production]
```

**Blue-green deployment**: Code deploys to staging slot first. Health check + warm-up. Only swaps to production after smoke test passes.

---

## 13. Deployment

### Single-Command Deployment

```powershell
.\infrastructure\scripts\deploy.ps1 `
    -Environment dev `
    -Prefix myplatform `
    -Location westus2 `
    -AiEndpoint "https://your-ai-endpoint.openai.azure.com" `
    -AiApiKey "your-key" `
    -AiDeploymentName "Phi-4"
```

### 9 Deployment Steps

| Step | What | Duration |
|------|------|----------|
| 1 | Pre-flight checks (az cli, login, func tools, password) | 5s |
| 2 | Register 9 Azure resource providers | 30s |
| 3 | Create resource group | 5s |
| 4 | Deploy Bicep (storage, KV, synapse, SQL, ADF, **2 function apps**, monitoring) | 10-15 min |
| 5 | Run 6 SQL scripts (schemas, catalog, config, audit, test, seed) | 1 min |
| 6 | Configure app settings for both Function Apps | 10s |
| 7a | Deploy BI Function App (`func publish {prefix}-{env}-func`) | 2-3 min |
| 7b | Deploy Test Function App (`func publish {prefix}-{env}-test-func`) | 2-3 min |
| 8 | Resume Synapse pool + verify health endpoints | 2 min |

**Independent Deployment**: Each product can be deployed separately:
```powershell
# BI Pipeline only
cd agents && func azure functionapp publish {prefix}-{env}-func --python

# Test Automation only
cd test-automation && func azure functionapp publish {prefix}-{env}-test-func --python
```

**Skip flags**: `-SkipInfra`, `-SkipSql`, `-SkipFuncDeploy`, `-SkipVerify`, `-PauseSynapseAfterDeploy`

---

## 14. Teams Integration

### Adaptive Card Types (4)

| Card | Trigger | Content |
|------|---------|---------|
| **Pipeline Started** | Orchestrator begins | Story ID, title, source tables, instance link |
| **Review Gate** | Planner completes | Mode, risk, artifact count, plan summary, **Approve/Decline action buttons** |
| **Progress Update** | Each step completes | Step status, elapsed time |
| **Pipeline Complete** | Orchestrator finishes | Success/failure, deployed objects, DQ scores |

---

## 15. Self-Correcting Loop: Bug Fixer (NEW in v5.0)

### The Closed Loop

```
Story -> Build -> Deploy -> Test -> Bug Found -> Auto-Fix -> Re-Test -> Green
                                        |                        ^
                                        v                        |
                                   ADO Bug #1234 -----> Bug Fixer Agent
```

### Bug Fix Orchestrator (8 Steps)

| Step | Name | What Happens |
|------|------|-------------|
| 1 | Fetch Bug from ADO | Reads bug title, description, repro steps, severity |
| 2 | Find Original Artifacts | Searches Config DB for the story/pipeline that produced the buggy code |
| 3 | Bug Fixer Agent | AI analyzes root cause, generates corrected SQL/ADF/recommendation |
| 4 | Code Review | AI reviews the fix using same 7-category review as original pipeline |
| 5 | Review Gate | Human approves/declines the fix before deployment |
| 6 | Deploy Fix | Corrected artifacts deployed to Synapse |
| 7 | Re-Test | Validation tests re-run to verify the fix |
| 8 | Update ADO Bug | Bug marked Resolved with fix details as comment |

### Fix Types

| Type | Description | Action |
|------|-------------|--------|
| **data_fix** | Wrong SQL logic, missing join, bad filter, null handling | Generates corrected SQL, deploys to Synapse |
| **pipeline_fix** | ADF config, source mapping, copy activity error | Generates corrected pipeline JSON, deploys to ADF |
| **ui_recommendation** | Power App issue (field, rule, formula) | Generates detailed fix recommendation, cannot auto-deploy |

### How It Works

1. Test automation runs, finds a bug, logs it to ADO (already implemented)
2. User clicks "Fix" in Web UI or calls `POST /api/fix-bug` with `{bug_id: 1234}`
3. Bug Fixer Agent reads the full bug context from ADO
4. Agent finds original artifacts from Config DB (knows what SQL was generated for that story)
5. LLM analyzes: bug description + original SQL + Synapse catalog = root cause + fix
6. Fix goes through the same Code Review Agent (7 categories)
7. Human Review Gate: reviewer sees the fix, the root cause, and the change summary
8. After approval, corrected artifacts deploy to Synapse
9. Validation tests re-run automatically to verify the fix
10. ADO Bug updated to Resolved with full fix report as comment

---

## 16. Ops Module (NEW in v6.0)

### Automated Operational Maintenance

The platform runs 3 timer-triggered jobs and exposes 7 HTTP endpoints for operational health.

### Timer Triggers (Automated)

| Schedule | Trigger | What It Does |
|----------|---------|-------------|
| Every 30 min | `auto_pause_synapse_timer` | Checks Config DB for recent activity. If no pipeline ran in 30 min, pauses Synapse pool. Sends Teams notification. Saves ~$870/mo if pool was left running 24/7. |
| Daily 8 AM UTC | `secret_health_check_timer` | Validates ADO PAT, AI Foundry key, SQL password. Sends Teams alert if any credential is expired or invalid. |
| Weekly Sun 2 AM | `weekly_cleanup_timer` | Purges execution_log entries older than 90 days. Keeps last 5 artifact versions per object. Cleans deployment_log. |

### HTTP Endpoints (7)

| Method | Route | Purpose |
|--------|-------|---------|
| GET | `/api/ops/dashboard` | Full dashboard: agent stats, secret health, Synapse idle check, DB counts, warnings |
| GET | `/api/ops/agent-stats?days=7` | Per-agent failure rates, avg duration, pipeline success rate |
| GET | `/api/ops/secret-health` | Credential validation: ADO PAT, AI key, SQL password, Teams webhook |
| GET | `/api/ops/synapse-idle?minutes=30` | Check if Synapse pool should be paused |
| POST | `/api/ops/pause-synapse` | Manually trigger Synapse pool pause |
| POST | `/api/ops/regression-test` | Run 3 known stories through LLM, validate output structure |
| POST | `/api/ops/cleanup` | Run DB retention cleanup (body: {retention_days: 90}) |

### Dashboard Response Structure

```json
{
  "timestamp": "2026-04-08T12:00:00",
  "platform_version": "6.0",
  "overall_health": "healthy | warning",
  "warnings": ["ADO PAT expires in 3 days", "Success rate below 80%"],
  "agent_stats": { "pipelines": { "total": 47, "completed": 44, "success_rate": 93.6 } },
  "secret_health": { "status": "healthy", "secrets": [...] },
  "synapse_idle": { "should_pause": false, "pool_status": "online" },
  "db_record_counts": { "pipeline_registry": 47, "execution_log": 312, "artifact_versions": 189 }
}
```

### Prompt Regression Testing

Runs 3 known test stories through the LLM and validates:
- JSON response is well-formed
- Required fields (story_id, mode, tables) are present
- Table list is non-empty

This catches LLM drift (model updates that change output format) before it hits production pipelines.

---

## 17. Integration Mode (Deep Dive)

### Onboarding a New Client

```
Step 1: POST /api/discover
        → Discovery Agent scans their Synapse, ADF, ADLS
        → Returns EnvironmentProfile (schemas, objects, naming patterns)

Step 2: POST /api/conventions   (body = discovery profile)
        → Convention Adapter auto-generates ConventionRuleset
        → Client can review/override rules in Web UI

Step 3: Submit stories normally (POST /api/process-story)
        → Planner/Developer agents receive ConventionRuleset as prompt context
        → Generated SQL uses client's naming conventions, schema names, distribution strategy

Step 4: POST /api/deliver-pr   (instead of direct deploy)
        → Creates branch + PR in client's ADO repo
        → Client reviews generated code, their CI/CD deploys
```

### Convention Adapter Rules (auto-detected)

| Rule | Example Detection | Effect on Generated Code |
|------|-------------------|--------------------------|
| Naming case | `raw_customer_orders` → snake_case | All objects use snake_case |
| Table prefix | 80% of tables start with `tbl_` | Generated tables get `tbl_` prefix |
| View prefix | 90% of views start with `vw_` | Generated views get `vw_` prefix |
| Bronze schema | Schema `raw` has only external tables | Bronze layer targets `[raw]` schema |
| Silver schema | Schema `cleansed` has tables + procs | Silver layer targets `[cleansed]` schema |
| Gold schema | Schema `analytics` is view-heavy | Gold layer targets `[analytics]` schema |
| Distribution | 70% use HASH distribution | Default distribution set to HASH |
| ADF naming | Pipelines follow `pl_{source}_{target}` | Generated pipelines match pattern |
| Delivery mode | Client has CI/CD pipeline | Artifacts delivered as PR, not deployed |

### PR Delivery Output Structure

```
client-repo/
  generated/
    STORY-001/
      bronze/
        ext_customer_orders.sql
      silver/
        tbl_customer_orders_cleansed.sql
        sp_load_customer_orders.sql
      gold/
        vw_customer_orders_summary.sql
      adf/
        pl_orders_bronze_to_silver.json
      README.md  (checklist: syntax, naming, logic, no breaking changes)
```

### Mode Comparison

| Aspect | Greenfield | Brownfield | Integration Mode |
|--------|-----------|------------|------------------|
| **Target** | New platform | Existing platform (we deployed) | Existing platform (client owns) |
| **Discovery** | N/A | Check existing tables | Full environment scan |
| **Conventions** | Our defaults | Our conventions | Client's auto-detected conventions |
| **Delivery** | Direct deploy | Direct deploy | Pull Request to client's repo |
| **Schemas** | bronze/silver/gold | bronze/silver/gold | Client's actual schema names |
| **CI/CD** | Our pipeline | Our pipeline | Client's existing CI/CD |
| **Risk** | None (new) | Low (additive) | Minimal (PR review gate) |

---

## 17. File Structure (Two-Product Monorepo)

```
synapse-bi-automation/
|-- infrastructure/
|   |-- bicep/
|   |   |-- main.bicep                    # Main deployment (9 modules)
|   |   |-- modules/
|   |   |   |-- storage.bicep             # ADLS Gen2 (GRS, deny by default)
|   |   |   |-- keyvault.bicep            # Key Vault (purge protection, 90d)
|   |   |   |-- keyvault-secrets.bicep    # Secrets population
|   |   |   |-- keyvault-rbac.bicep       # RBAC assignments
|   |   |   |-- synapse.bicep             # Synapse workspace + dedicated pool
|   |   |   |-- sql-source.bicep          # Azure SQL (source + config DB)
|   |   |   |-- function-app.bicep        # BI Function App (KV refs, managed identity)
|   |   |-- function-app-test.bicep  # Test Function App (shares ASP)
|   |   |   |-- data-factory.bicep        # ADF + linked services
|   |   |   |-- monitoring.bicep          # Log Analytics + App Insights
|   |   |   |-- openai.bicep              # Azure OpenAI (optional)
|   |   |   |-- bot-service.bicep         # Bot Service (not in main)
|   |   |   |-- container-registry.bicep  # ACR (not in main)
|   |   |-- parameters/
|   |       |-- dev.bicepparam
|   |       |-- prod.bicepparam
|   |-- scripts/
|   |   |-- deploy.ps1                    # Single-command deployment (8 steps)
|   |   |-- deploy.sh                     # Bash deployment script
|   |-- sql/
|   |   |-- test_schema.sql               # Test automation tables
|   |-- docker/
|       |-- playwright-edge/              # Dockerfile for ACI (future use)
|
|-- agents/                                # PRODUCT 1: BI Pipeline Automation
|   |-- function_app.py                   # 31 endpoints, 1 orchestrator, 20 activities (2914 lines)
|   |-- host.json                         # BiAutoHubV8, 30-min timeout
|   |-- requirements.txt                  # 15 pinned dependencies
|   |-- startup.sh                        # ODBC driver install
|   |-- blueprints/
|   |   |-- common.py                     # Shared utilities
|   |   |-- admin_bp.py                   # Health, UI, history endpoints
|   |   |-- catalog_bp.py                 # Lineage, catalog, NL query, DQ
|   |   |-- pipeline_bp.py               # Approve, decline, cancel
|   |-- shared/
|   |   |-- config.py                     # AppConfig (fail-fast, no hardcoded defaults)
|   |   |-- llm_client.py                # LLM wrapper (retry, token tracking, JSON fallback)
|   |   |-- synapse_client.py            # Synapse SQL (parameterized queries)
|   |   |-- models.py                     # 16 Pydantic models
|   |   |-- ado_client.py                # ADO REST API
|   |   |-- adf_client.py               # ADF management
|   |   |-- state_registry.py            # Config DB operations
|   |   |-- story_mapper.py              # Story routing (3 paths)
|   |   |-- story_interpreter.py         # Universal format interpreter
|   |   |-- artifact_versioner.py        # Git commit artifacts
|   |   |-- connector_client.py          # Multi-source connectors
|   |   |-- data_quality.py             # DQ framework
|   |   |-- lineage_tracker.py          # Column-level lineage
|   |   |-- teams_bot.py                # Bot Framework
|   |   |-- teams_webhook.py            # Adaptive card builder
|   |   |-- sharepoint_client.py        # Graph API client
|   |   |-- ops.py                       # Ops Module (454 lines): dashboard, auto-pause, secrets, regression, cleanup
|   |-- planner/
|   |   |-- agent.py                     # PlannerAgent
|   |   |-- prompts/                     # LLM prompts (story_parser, plan_generator)
|   |-- developer/
|   |   |-- agent.py                     # DeveloperAgent
|   |   |-- prompts/                     # LLM prompts (sql_generator, adf_generator)
|   |   |-- templates/                   # SQL templates (bronze/silver/gold)
|   |-- validator/
|   |   |-- agent.py                     # ValidatorAgent
|   |-- healer/
|   |   |-- agent.py                     # HealerAgent (validation + code review healing)
|   |   |-- prompts/                     # LLM prompts (heal_sql)
|   |-- reviewer/
|   |   |-- agent.py                     # CodeReviewAgent (7 check categories)
|   |-- discovery/
|   |   |-- agent.py                     # Discovery Agent (scans Synapse/ADF/ADLS/Power BI)
|   |-- commander/
|   |   |-- agent.py                     # Commander Agent (task decomposition, dynamic dispatch)
|   |   |-- orchestrator.py              # Commander Durable Functions orchestrator
|   |-- supervisor/
|   |   |-- agent.py                     # Supervisor Agent (independent quality watchdog)
|   |-- fixer/
|   |   |-- agent.py                     # Bug Fixer Agent (reads ADO bug, generates corrected code)
|   |-- static/
|       |-- index.html                   # 15-page SPA (1856 lines)
|
|-- test-automation/                       # PRODUCT 2: Test Automation Platform
|   |-- function_app.py                   # 11 endpoints, 1 orchestrator, 9 activities (664 lines)
|   |-- host.json                         # BiAutoTestHubV1 (separate Durable Task hub)
|   |-- requirements.txt                  # Independent dependencies
|   |-- startup.sh                        # ODBC driver install
|   |-- shared/                           # Copies of shared modules
|   |   |-- config.py, llm_client.py, synapse_client.py, ado_client.py, etc.
|   |-- tester/
|       |-- test_router.py               # AI test type classifier
|       |-- test_planner.py              # UI test scenario generator
|       |-- test_generator.py            # Playwright code generator
|       |-- test_executor.py             # ACI/local execution trigger
|       |-- data_test_planner.py         # SQL test generator
|       |-- data_test_executor.py        # Server-side SQL execution
|       |-- test_reporter.py             # ADO + Teams reporting
|       |-- ado_test_client.py           # ADO Test Management API
|       |-- local-agent/                 # QA machine package
|           |-- setup.py                 # bi-test-agent package
|           |-- agent/
|               |-- cli.py               # CLI (connect, run, setup)
|               |-- runner.py            # Job polling + execution
|               |-- executor.py          # Playwright headed Edge
|               |-- report.py            # Excel report generation
|
|-- catalog/
|   |-- ddl/
|   |   |-- 01_create_schemas.sql        # bronze, silver, gold, catalog, audit, config
|   |   |-- 02_create_catalog_tables.sql # source_systems, source_tables, joins, glossary
|   |   |-- 03_create_config_tables.sql  # pipeline_registry, execution_log, artifacts
|   |   |-- 03_create_audit_tables.sql   # agent_log, validation_results, healer_actions
|   |   |-- 99_drop_all_objects.sql      # Full cleanup script
|   |-- seed/
|       |-- 01_seed_source_data.sql      # 435 sample records
|       |-- 02_seed_catalog_metadata.sql # Source system + table metadata
|
|-- pipelines/
|   |-- ci.yml                           # Lint, test, security scan, coverage gate
|   |-- cd-dev.yml                       # Blue-green deploy (staging -> swap)
|
|-- tests/
|   |-- conftest.py                      # Fixtures (mock_config, mock_llm, sample data)
|   |-- unit/
|   |   |-- test_config.py              # 8 tests
|   |   |-- test_llm_client.py          # 12 tests
|   |   |-- test_synapse_client.py      # 9 tests
|   |   |-- test_story_mapper.py        # 8 tests
|   |   |-- test_connector_client.py    # 5 tests
|   |   |-- test_reviewer_agent.py      # 3 tests
|   |-- integration/                     # (placeholder for Azure-connected tests)
|
|-- docs/
|   |-- architecture.md                  # This document
|   |-- setup-guide.md                   # Deployment guide
|   |-- lift-and-shift-guide.md          # Migration guide
|   |-- demo-deck.html                   # 16-slide HTML presentation
|   |-- platform-flow.png               # End-to-end flow diagram (4000x3850, EY theme)
|   |-- generate_flow.py                # Script to regenerate flow diagram
|   |-- competitive-analysis.md          # 7 competitor categories comparison
|   |-- executive-deck.pptx              # 22-slide PowerPoint (EY branded)
|
|-- pytest.ini                           # Test configuration
|-- .gitignore
|-- README.md
```

---

## 18. Counts Summary

| Category | BI Pipeline | Test Automation | Total |
|----------|-------------|-----------------|-------|
| **Function Apps** | 1 | 1 | **2** |
| **HTTP Endpoints** | 48 (+4 commander, +7 ops, +3 bug fixer, +3 integration) | 11 | **59** |
| **Durable Orchestrators** | 3 (story: 9 steps, commander: dynamic, bug fix: 8 steps) | 1 (6 steps) | **4** |
| **Timer Triggers** | 3 (auto-pause, secret check, cleanup) | 0 | **3** |
| **Activity Functions** | 28 (+8 commander/supervisor activities) | 9 | **37** |
| **AI Agents** | 2 Command + 5 Worker + 1 Discovery + 1 Bug Fixer | 6 | **15** |
| **Shared Modules** | 8 (incl. convention_adapter, pr_client, ops) | 6 (copies) | **14** |

| Category | Count |
|----------|-------|
| Azure Resources | 13 types deployed (incl. 2 Function Apps) |
| Integration Mode Components | 3 (Discovery Agent, Convention Adapter, PR Client) |
| Database Schemas | 7 (bronze, silver, gold, catalog, audit, config, dbo) |
| Database Tables | 20 |
| Pydantic Models | 16 |
| Web UI Pages | 15 |
| Unit Tests | 155 |
| Teams Card Types | 4 |
| Durable Task Hubs | 3 (BiAutoHubV8, BiAutoTestHubV1, CommanderHub) |

### Deployment Independence

| Aspect | BI Pipeline | Test Automation |
|--------|-------------|-----------------|
| Deploy command | `func publish {prefix}-{env}-func` | `func publish {prefix}-{env}-test-func` |
| Durable Task Hub | BiAutoHubV8 | BiAutoTestHubV1 |
| Can deploy alone | Yes | Yes |
| Shared infra | ASP, Storage, Key Vault, App Insights | Same |
| Synapse dependency | Required (deploys SQL) | Optional (data tests only) |
| ADO dependency | Required (fetches stories) | Required (creates test artifacts) |
| LLM dependency | Required (all agents) | Required (router, planners, generator) |
