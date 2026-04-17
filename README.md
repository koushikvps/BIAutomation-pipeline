# Synapse BI Automation Framework

Multi-agent automation framework that reads business stories and builds Azure Synapse medallion architecture (Bronze → Silver → Gold views). Two independent products — BI Pipeline Automation (9 agents) and Test Automation Platform (6 agents) — sharing Azure infrastructure.

## Architecture (Commander + Supervisor)

The platform uses a **Commander + Supervisor** architecture where a Commander Agent dynamically orchestrates all worker agents and an independent Supervisor Agent enforces quality gates.

### Command Layer
- **Commander Agent**: Task decomposition, dynamic agent dispatch, result evaluation, failure recovery
- **Supervisor Agent**: Independent quality watchdog, SLA enforcement, plan validation, final sign-off

### Worker Agents (BI Pipeline)
- **Planner Agent**: Parses stories, detects mode (greenfield/brownfield), creates build plans
- **Developer Agent**: Generates ADF pipelines, DDL, stored procedures, views
- **Code Review Agent**: Reviews generated code across 7 quality categories
- **Validator Agent**: Pre/post deployment data quality checks
- **Healer Agent**: Auto-remediates failures, escalates when needed
- **Discovery Agent**: Scans existing platforms for Integration Mode
- **Bug Fixer Agent**: Self-correcting loop for ADO bugs

### Test Automation Agents
- **Test Router**: AI classifies stories as UI/Data/Both
- **Test Planner + Data Test Planner**: Generate test scenarios
- **Test Generator**: Playwright script generation
- **Data Test Executor**: Server-side SQL validation
- **Test Reporter**: ADO + Teams + Excel reporting

### Operational
- **Ops Module**: Auto-pause Synapse, secret health checks, weekly cleanup, regression tests

## Endpoints
- BI Pipeline: 48 HTTP endpoints + 4 Commander endpoints + 3 timers
- Test Automation: 11 HTTP endpoints

## Tests
155 unit tests (all passing)

## Prerequisites
- Azure Visual Studio Enterprise subscription
- Azure CLI installed
- Python 3.10+
- Azure Functions Core Tools v4

## Quick Start
See `docs/setup-guide.md` for step-by-step deployment instructions.
See `docs/architecture.md` for detailed architecture documentation (v7.0).
