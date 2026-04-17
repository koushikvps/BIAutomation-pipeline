# BI Pipeline Agent Audit Report

**Date:** 2026-04-15  
**Scope:** `/project/workspace/synapse-bi-automation/agents/`

---

## 1. Agent-by-Agent Audit

### 1.1 Planner Agent (`planner/agent.py` — 304 lines)

| Criterion | Assessment |
|---|---|
| **Calls LLM?** | ✅ YES — `self._llm.chat_json()` at lines ~80 (story parsing) and ~157 (plan generation) |
| **Error handling?** | ✅ GOOD — try/except around Synapse queries (lines ~32-53, ~98-130), LLM fallback plan on failure (line ~163), non-critical loads wrapped |
| **Input validation?** | ⚠️ PARTIAL — accepts `str | dict` but no schema validation on dict input; StoryContract Pydantic model provides some validation |
| **Prompts well-structured?** | ✅ YES — prompts loaded from external files (`prompts/story_parser.txt`, `prompts/plan_generator.txt`), user prompt is structured JSON |
| **Complexity** | HIGH — full pipeline: parse story → detect mode (greenfield/brownfield/partial/hybrid) → query Synapse metadata → generate build plan with LLM + template fallback |

**Issues:**
- Line ~36-52: `_load_feedback()` opens raw pyodbc connection bypassing `SynapseClient`, duplicating connection logic and credentials
- Line ~93-129: `_detect_mode()` also opens raw pyodbc connections bypassing `SynapseClient`, accesses private attributes (`self._synapse._endpoint`, `self._synapse._password`) — violates encapsulation
- Line ~163: Template fallback plan is hardcoded for a "sales" domain — may produce incorrect plans for other domains

---

### 1.2 Developer Agent (`developer/agent.py` — 516 lines)

| Criterion | Assessment |
|---|---|
| **Calls LLM?** | ✅ YES — `self._llm.chat()` for SQL generation (line ~289), `self._llm.chat()` for ADF pipeline generation (line ~296) |
| **Error handling?** | ⚠️ PARTIAL — `_get_source_columns()` has try/except (line ~32-65), but `run()` method has no top-level error handling; `_generate_artifact()` returns `None` silently on some paths |
| **Input validation?** | ⚠️ WEAK — trusts BuildPlan model validation from Pydantic but doesn't validate column data types or step ordering |
| **Prompts well-structured?** | ✅ YES — prompts from files (`prompts/sql_generator.txt`, `prompts/adf_generator.txt`), user prompt is structured JSON |
| **Complexity** | VERY HIGH — generates bronze external tables, silver CTAS with joins/aggregations, gold views with semantic definitions, ADF pipeline JSON with watermark support |

**Issues:**
- Line ~32-65: `_get_source_columns()` opens raw pyodbc connections, duplicating connection logic (same pattern as Planner)
- Line ~68-92: `_load_semantic_definitions()` opens raw pyodbc connections — same issue
- Line ~160-240: `_build_silver_table()` has hardcoded join pairs (`OrderHeader`→`OrderDetail`→`Product`→`Customer`) — only works for sales domain, not generic
- Line ~117: `_generate_artifact()` returns `None` for silver stored procedures without columns — potential silent failure that may confuse downstream

---

### 1.3 Validator Agent (`validator/agent.py` — 327 lines)

| Criterion | Assessment |
|---|---|
| **Calls LLM?** | ❌ NO — pure logic-based validation (regex, SQL queries, JSON parsing). No LLM dependency. |
| **Error handling?** | ✅ GOOD — each check type wrapped in try/except (lines ~56-68), errors create FAIL ValidationCheck entries |
| **Input validation?** | ✅ GOOD — validates SQL syntax via regex blocklist, naming conventions, JSON structure, dependencies |
| **Prompts well-structured?** | N/A — no prompts used |
| **Complexity** | MEDIUM-HIGH — pre-deploy static checks (SQL blocklist, naming, ADF JSON, dependencies) + post-deploy runtime checks (row counts, nulls, duplicates, reconciliation) |

**Issues:**
- Line ~14-18: `SYNAPSE_BLOCKLIST` blocks `SELECT *` which may be overly strict for some valid use cases in external table definitions
- Line ~140-183: `_check_nulls()` and `_check_duplicates()` use string formatting for schema/table in SQL (e.g., `f"WHERE TABLE_SCHEMA = '{schema}'"`) — potential SQL injection if object names contain single quotes. Should use parameterized queries consistently.
- Line ~128-129: `_check_row_count()` has confusing logic where `source_sql` and `target_sql` may be set to the same value

---

### 1.4 Healer Agent (`healer/agent.py` — 333 lines)

| Criterion | Assessment |
|---|---|
| **Calls LLM?** | ✅ YES — `self._llm.chat_json()` for SQL healing (line ~167) and code review healing (line ~225) |
| **Error handling?** | ✅ GOOD — max retry limit (3), escalation for non-healable failures, try/except around catalog queries |
| **Input validation?** | ✅ GOOD — classifies failures by type/severity, checks auto-healability before attempting fix |
| **Prompts well-structured?** | ✅ YES — prompt from file (`prompts/heal_sql.txt`), context includes original SQL, error message, catalog context; code review healing builds structured inline prompt |
| **Complexity** | HIGH — two healing modes: validation-based and code-review-based, with artifact replacement and escalation tracking |

**Issues:**
- No significant issues found. Well-structured with clear separation of concerns.

---

### 1.5 Code Review Agent (`reviewer/agent.py` — 194 lines)

| Criterion | Assessment |
|---|---|
| **Calls LLM?** | ✅ YES — `self._llm.chat_json()` for review (line ~152) |
| **Error handling?** | ⚠️ PARTIAL — `result.setdefault()` provides defaults if LLM returns incomplete JSON, but no try/except around the LLM call itself |
| **Input validation?** | ⚠️ WEAK — trusts LLM to return structured JSON; finding counts are recalculated from reviews which is good, but no validation of artifact content |
| **Prompts well-structured?** | ✅ EXCELLENT — inline SYSTEM_PROMPT (lines ~18-110) is very detailed with 7 review categories, clear output schema, and rules for verdict decisions |
| **Complexity** | MEDIUM — single LLM call with structured prompt, post-processing of results |

**Issues:**
- Line ~152: No try/except around `self._llm.chat_json()` — if LLM fails, entire review throws unhandled exception
- Line ~130-145: All artifacts concatenated into single prompt — may exceed token limits for large artifact bundles

---

### 1.6 Discovery Agent (`discovery/agent.py` — 472 lines)

| Criterion | Assessment |
|---|---|
| **Calls LLM?** | ❌ NO — pure infrastructure scanning (Synapse metadata, ADF REST API, ADLS file listing). No LLM dependency. |
| **Error handling?** | ✅ GOOD — each discovery section (Synapse, ADF, ADLS) wrapped in try/except, non-critical errors logged as warnings |
| **Input validation?** | ✅ GOOD — options dict allows skipping sections, defensive coding throughout |
| **Prompts well-structured?** | N/A — no prompts used |
| **Complexity** | HIGH — scans Synapse schemas/tables/views/procs/external tables with distributions, ADF pipelines/datasets/triggers via REST, ADLS containers/folders, auto-detects naming conventions |

**Issues:**
- Line ~127-175: ADF discovery uses raw `requests.get()` calls with `DefaultAzureCredential` — no retry logic for Azure API calls (unlike SynapseClient which has retries)
- Line ~285-340: Convention detection is regex-based heuristic — may misclassify objects in non-standard schemas

---

### 1.7 Bug Fixer Agent (`fixer/agent.py` — 162 lines)

| Criterion | Assessment |
|---|---|
| **Calls LLM?** | ✅ YES — `self._llm.chat_json()` for root cause analysis and fix generation (line ~108) |
| **Error handling?** | ⚠️ PARTIAL — `_get_catalog_context()` has try/except, but `analyze_and_fix()` doesn't handle LLM failure |
| **Input validation?** | ⚠️ WEAK — accepts arbitrary dict for bug_details with no schema validation; relies on LLM to produce valid output |
| **Prompts well-structured?** | ✅ GOOD — inline `FIX_SYSTEM_PROMPT` (lines ~25-63) is detailed with rules for data/pipeline/UI fix types, confidence levels |
| **Complexity** | MEDIUM — single-purpose: read bug → gather catalog context → LLM generates fix → return result |

**Issues:**
- Line ~108: No try/except around the LLM call — unhandled exception if LLM fails
- Line ~140-160: `_get_catalog_context()` extracts `[schema].[object]` patterns from bug text using regex — could miss non-bracket-delimited references

---

### 1.8 Commander Agent (`commander/agent.py` — 258 lines)

| Criterion | Assessment |
|---|---|
| **Calls LLM?** | ✅ YES — `self._llm.chat_json()` for plan creation (line ~109), result evaluation (line ~148), and failure handling (line ~185) |
| **Error handling?** | ✅ GOOD — try/except around each LLM call with sensible fallback defaults; max retries enforced |
| **Input validation?** | ⚠️ PARTIAL — `plan_execution()` accepts dict with `.get()` but no formal schema; result evaluation trusts LLM output |
| **Prompts well-structured?** | ✅ GOOD — three distinct system prompts for planning, evaluation, and failure handling; includes available agent descriptions and rules |
| **Complexity** | HIGH — meta-agent that dynamically plans agent execution order, evaluates results, and handles failures with rerouting capability |

**Issues:**
- Line ~68: `AVAILABLE_AGENTS` dict has entries like `"deployer_adf"`, `"deployer_sql"`, `"pr_delivery"`, `"notify_teams"` — these are not standalone agent.py files but activity functions in function_app.py. The commander dispatch function (function_app.py line ~4065) maps these names but uses **different method signatures** than the actual agents (e.g., calls `PlannerAgent(config).create_plan()` which doesn't exist — the actual method is `.run()`). **This is a significant bug.**

---

### 1.9 Supervisor Agent (`supervisor/agent.py` — 279 lines)

| Criterion | Assessment |
|---|---|
| **Calls LLM?** | ✅ YES — `self._llm.chat_json()` for plan checking (line ~112), step checking (line ~161), and final signoff (line ~204) |
| **Error handling?** | ✅ EXCELLENT — every LLM call has try/except with graceful fallback (approve with warning); SLA checks are non-LLM pre-checks; LLM budget tracking |
| **Input validation?** | ✅ GOOD — SLA enforcement (duration, retries, LLM calls, cost) before LLM evaluation; non-critical agents auto-approved |
| **Prompts well-structured?** | ✅ GOOD — three inline system prompts for plan validation, step validation, and final signoff with clear rules and output schemas |
| **Complexity** | MEDIUM-HIGH — independent watchdog with SLA enforcement, step-by-step quality validation, audit trail, and override capability |

**Issues:**
- Line ~47: `SLAConfig.max_cost_usd = 5.0` is tracked but never actually calculated — no cost accumulation logic exists
- Line ~53: `SupervisorAgent` creates a new instance per activity call in function_app.py, so `_step_verdicts`, `_llm_calls`, `_total_retries`, `_start_time` are all reset each call — audit trail is not preserved across steps. **The stateful design doesn't work with Azure Functions' stateless activity model.**

---

## 2. function_app.py Endpoint Audit (4,187 lines)

### 2.1 Total Endpoints Found

| Route | Method | Auth Level | Category |
|---|---|---|---|
| `/api/health` | GET | ANONYMOUS | Health/Monitoring |
| `/api/ui` | GET | ANONYMOUS | UI |
| `/api/pipeline-history` | GET | FUNCTION | Data |
| `/api/artifact-history` | GET | FUNCTION | Data |
| `/api/data-lineage` | GET | FUNCTION | Data |
| `/api/data-catalog` | GET | FUNCTION | Data |
| `/api/nl-query` | POST | FUNCTION | Query |
| `/api/templates` | GET | FUNCTION | Templates |
| `/api/use-template` | POST | FUNCTION | Templates |
| `/api/semantic` | GET, POST | FUNCTION | Semantic |
| `/api/feedback` | GET, POST | FUNCTION | Feedback |
| `/api/schedules` | GET | FUNCTION | ADF |
| `/api/costs` | GET | FUNCTION | Cost |
| `/api/notify` | POST | FUNCTION | Teams |
| `/api/interpret-story` | POST | FUNCTION | Story |
| `/api/process-free-story` | POST | FUNCTION | Story |
| `/api/connectors` | GET, POST | FUNCTION | Connectors |
| `/api/generate-pbi` | POST | FUNCTION | Power BI |
| `/api/column-lineage` | GET | FUNCTION | Lineage |
| `/api/data-quality` | POST | FUNCTION | DQ |
| `/api/approve-plan` | POST | FUNCTION | Review Gate |
| `/api/decline-plan` | POST | FUNCTION | Review Gate |
| `/api/cancel-pipeline` | POST | FUNCTION | Pipeline Control |
| `/api/process-story` | POST | FUNCTION | Pipeline |
| `/api/preview-ado-story` | POST | FUNCTION | ADO |
| `/api/process-ado-story` | POST | FUNCTION | Pipeline |
| `/api/fix-bug` | POST | FUNCTION | Bug Fix |
| `/api/fix-status/{id}` | GET | FUNCTION | Bug Fix |
| `/api/fix-approve/{id}` | POST | FUNCTION | Bug Fix |
| `/api/discover` | POST | FUNCTION | Discovery |
| `/api/conventions` | GET, POST | FUNCTION | Conventions |
| `/api/deliver-pr` | POST | FUNCTION | PR Delivery |
| `/api/ops/dashboard` | GET | FUNCTION | Ops |
| `/api/ops/agent-stats` | GET | FUNCTION | Ops |
| `/api/ops/secret-health` | GET | FUNCTION | Ops |
| `/api/ops/synapse-idle` | GET | FUNCTION | Ops |
| `/api/ops/pause-synapse` | POST | FUNCTION | Ops |
| `/api/ops/regression-test` | POST | FUNCTION | Ops |
| `/api/ops/cleanup` | POST | FUNCTION | Ops |
| `/api/bot-message` | POST | ANONYMOUS | Teams Bot |
| `/api/bot-notify` | POST | FUNCTION | Teams Bot |
| `/api/pipeline-status` | GET | FUNCTION | Pipeline |
| `/api/story-status` | GET | FUNCTION | Pipeline |
| `/api/pipeline-progress` | GET, POST | FUNCTION | Pipeline |
| `/api/commander/run` | POST | FUNCTION | Commander |
| `/api/commander/status` | GET | FUNCTION | Commander |
| `/api/commander/approve` | POST | FUNCTION | Commander |
| `/api/commander/decline` | POST | FUNCTION | Commander |

**Total: 47 HTTP endpoints + 3 timer triggers + 2 orchestrators + ~20 activity functions**

### 2.2 Commander Endpoints Status

All 4 commander endpoints are **fully wired and functional**:
- `commander/run` → starts `commander_orchestrator` (line 3842)
- `commander/status` → reads Durable Functions status (line 3783)
- `commander/approve` → raises `CommanderReviewApproved` event (line 3805)
- `commander/decline` → raises `CommanderReviewApproved` with `approved: False` (line 3820)

**Critical Bug in Commander Dispatch** (line ~4065-4140): `commander_dispatch_agent()` calls methods that don't exist on the actual agents:
- Calls `PlannerAgent(config).create_plan()` — actual method is `.run()`
- Calls `DeveloperAgent(config).generate_artifacts()` — actual method is `.run()`
- Calls `CodeReviewAgent(config).review(artifacts=...)` — actual signature is `.review(artifacts, build_plan)`
- Calls `ValidatorAgent(config).validate()` — actual methods are `.pre_deploy_check()` and `.post_deploy_check()`
- Calls `HealerAgent(config).heal()` — actual method is `.run()` or `.heal_from_review()`
- Calls `DiscoveryAgent(config).scan()` — actual method is `.discover()`

**This means the Commander pipeline would crash at runtime when dispatching any agent.**

---

## 3. Shared Modules Audit

| Module | Lines | Assessment |
|---|---|---|
| `config.py` | 56 | ✅ Real — `AppConfig` dataclass with `from_env()`, requires env vars |
| `models.py` | 182 | ✅ Real — comprehensive Pydantic models for all data contracts |
| `llm_client.py` | 185 | ✅ Real — OpenAI client with retries, token tracking, JSON extraction, Key Vault fallback |
| `synapse_client.py` | 174 | ✅ Real — pyodbc with retry logic, transient error detection, GO splitting, DDL execution |
| `adf_client.py` | 237 | ✅ Real — ADF REST API deployment with datasets, triggers, linked service creation |
| `ado_client.py` | 107 | ✅ Real — ADO work item fetch/update/comment via REST API |
| `state_registry.py` | 257 | ✅ Real — Config DB CRUD for pipeline state, artifact versions, step logging |
| `story_mapper.py` | 239 | ✅ Real — Maps ADO work items to StoryContract with LLM interpretation |
| `story_interpreter.py` | 298 | ✅ Real — Interprets free-text/Gherkin stories into StoryContract with format detection |
| `connector_client.py` | 208 | ✅ Real — Data source connector CRUD with preview/test for SQL, REST, CSV, Excel |
| `convention_adapter.py` | 190 | ✅ Real — Builds ConventionRuleset from discovery profile, generates prompt context |
| `data_quality.py` | 213 | ✅ Real — DQ checks (freshness, completeness, uniqueness, referential integrity) |
| `lineage_tracker.py` | 177 | ✅ Real — Column-level lineage extraction from SQL + Config DB persistence |
| `ops.py` | 456 | ✅ Real — Operational dashboard, agent stats, secret health, Synapse auto-pause, regression tests, cleanup |
| `pr_client.py` | 214 | ✅ Real — ADO Git PR creation with branch management via REST API |
| `artifact_versioner.py` | 173 | ✅ Real — Commits artifacts to ADO Git repo |
| `teams_bot.py` | 650 | ✅ Real — Bot Framework bot with natural language command parsing, adaptive cards |
| `teams_webhook.py` | 320 | ✅ Real — Teams webhook notifications with adaptive cards for various pipeline stages |
| `__init__.py` | 1 | ✅ Package marker |

**All 19 shared modules are real implementations, none are stubs.**

---

## 4. UI Audit (`static/index.html` — 141,072 bytes)

### 4.1 API Calls Found in UI

The UI calls **39 distinct API endpoints** covering:
- Pipeline operations: `process-free-story`, `process-ado-story`, `pipeline-progress`, `approve-plan`, `decline-plan`, `cancel-pipeline`
- Data browsing: `data-lineage`, `data-catalog`, `nl-query`, `column-lineage`
- Templates: `templates`, `use-template`
- Management: `semantic`, `feedback`, `connectors`, `schedules`, `costs`
- Operations: `ops/dashboard`, `ops/pause-synapse`, `ops/cleanup`, `ops/regression-test`, `ops/secret-health`
- Power BI: `generate-pbi`
- Health: `health`, `pipeline-history`
- Test automation: `test-categories`, `run-tests`, `test-progress`, `agent-log`, `download-tests`, `upload-results`, `queue-agent-job`, `approve-test-plan`, `decline-test-plan`, `download-data-report`

### 4.2 Commander Endpoints in UI

**❌ The UI does NOT call any commander endpoints.** A `grep -i commander` on index.html returns zero matches.

The UI uses the **hardcoded orchestrator pipeline** (`process-story`, `process-ado-story`, `process-free-story`) and the standard review gates (`approve-plan`, `decline-plan`), but has **no integration** with:
- `/api/commander/run`
- `/api/commander/status`
- `/api/commander/approve`
- `/api/commander/decline`

---

## 5. Critical Issues Summary

### 🔴 P0 — Commander Agent Dispatch is Broken
**File:** `function_app.py` lines 4065-4140 (`commander_dispatch_agent`)  
The commander dispatch function calls methods that don't exist on the actual agent classes. Every agent dispatch would crash at runtime with `AttributeError`. The hardcoded orchestrator works correctly because it uses the right activity function wiring.

### 🟡 P1 — Supervisor Agent Statefulness Doesn't Work
**File:** `supervisor/agent.py`  
The Supervisor is designed as a stateful class (`_step_verdicts`, `_start_time`, `_llm_calls`) but is instantiated fresh per activity function call in Azure Functions. The audit trail, retry tracking, and timing are reset every call.

### 🟡 P1 — Raw pyodbc Connections Bypass SynapseClient
**Files:** `planner/agent.py` (lines 32-53, 93-129), `developer/agent.py` (lines 32-65, 68-92)  
Multiple agents open raw pyodbc connections instead of using `SynapseClient`, duplicating connection string logic, credential handling, and retry logic. They also access private `_endpoint`/`_password` attributes.

### 🟡 P1 — UI Has No Commander Integration
**File:** `static/index.html`  
The Commander pipeline endpoints exist in function_app.py but are not accessible from the web UI. Users can only trigger the hardcoded orchestrator.

### 🟠 P2 — SQL Injection Risk in Validator
**File:** `validator/agent.py` lines 140-183  
`_check_nulls()` and `_check_duplicates()` use string formatting for schema/table names in SQL queries instead of parameterized queries.

### 🟠 P2 — Hardcoded Sales Domain Logic in Developer
**File:** `developer/agent.py` lines 160-240  
Silver table generation has hardcoded join pairs (`OrderHeader`→`OrderDetail`→`Product`→`Customer`) that only work for the sales domain.

### 🟠 P2 — No LLM Error Handling in Reviewer and Fixer
**Files:** `reviewer/agent.py` line 152, `fixer/agent.py` line 108  
LLM calls in `CodeReviewAgent.review()` and `BugFixerAgent.analyze_and_fix()` have no try/except — unhandled exceptions propagate up.

---

## 6. Code Metrics

| Component | Lines | LLM Calls | Prompt Files | Complexity |
|---|---|---|---|---|
| Planner Agent | 304 | 2 | 2 | High |
| Developer Agent | 516 | 2 | 2 | Very High |
| Validator Agent | 327 | 0 | 0 | Medium-High |
| Healer Agent | 333 | 2+ | 1 | High |
| Code Review Agent | 194 | 1 | 0 (inline) | Medium |
| Discovery Agent | 472 | 0 | 0 | High |
| Bug Fixer Agent | 162 | 1 | 0 (inline) | Medium |
| Commander Agent | 258 | 3 | 0 (inline) | High |
| Supervisor Agent | 279 | 3 | 0 (inline) | Medium-High |
| function_app.py | 4,187 | 0 (delegates) | 0 | Very High |
| shared/ (19 modules) | 4,337 | 1 (story_interpreter) | 0 | High |
| **Total** | **11,369** | **~15** | **5** | — |
