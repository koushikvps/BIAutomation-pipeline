# Setup Guide: Clone to Deployed in 30 Minutes

**Platform Version**: 7.0
**Last Updated**: 2026-04-15
**Products**: BI Pipeline (9 agents, incl. Commander + Supervisor) + Test Automation (6 agents)

---

## Prerequisites

| Tool | Minimum Version | How to Check |
|------|----------------|--------------|
| Azure CLI | 2.50+ | `az --version` |
| Python | 3.10+ | `python3 --version` |
| Azure Functions Core Tools | 4.x | `func --version` |
| Git | 2.x | `git --version` |
| Node.js (for func tools) | 18+ | `node --version` |
| ODBC Driver 18 for SQL Server | 18 | `odbcinst -q -d` |

**Azure Requirements:**
- Active Azure subscription (VS Enterprise, Pay-As-You-Go, or EA)
- Permissions: Contributor + User Access Administrator on the subscription
- Region: West US 2 (default) — or any region with Azure AI Foundry

---

## Quick Start (5 commands)

```bash
# 1. Clone and navigate
git clone https://dev.azure.com/EYCoreAssurance/Assurance%20Datawarehouse/_git/synapse-bi-automation
cd synapse-bi-automation

# 2. Login to Azure
az login
az account set --subscription "<YOUR_SUBSCRIPTION_ID>"

# 3. Deploy infrastructure
cd infrastructure/scripts && chmod +x deploy.sh && ./deploy.sh dev

# 4. Deploy BI Pipeline Function App
cd ../../agents && func azure functionapp publish biautomation-dev-func --python

# 5. Deploy Test Automation Function App
cd ../test-automation && func azure functionapp publish biautomation-dev-test-func --python
```

---

## Detailed Steps

### Step 1: Clone Repository

```bash
git clone https://dev.azure.com/EYCoreAssurance/Assurance%20Datawarehouse/_git/synapse-bi-automation
cd synapse-bi-automation
```

Repository structure:
```
synapse-bi-automation/
├── agents/                  # Product 1: BI Pipeline (48 endpoints + 3 timers, Commander+Supervisor)
├── test-automation/         # Product 2: Test Automation (11 endpoints)
├── infrastructure/          # Bicep IaC (17 modules)
├── catalog/                 # DDL + seed SQL scripts
├── pipelines/               # ADO CI/CD YAML
├── tests/                   # 155 unit tests
└── docs/                    # Architecture, this guide, deck
```

### Step 2: Configure Parameters

Edit `infrastructure/bicep/parameters/dev.bicepparam`:

```bicep
using '../main.bicep'

param environment = 'dev'
param prefix = 'biautomation'          // Change for your org
param location = 'westus2'
param synapseSqlAdminObjectId = ''     // Will be auto-filled by deploy.sh
param sqlAdminUsername = 'sqladmin'
param sqlAdminPassword = '<SET_STRONG_PASSWORD>'  // Min 12 chars, upper+lower+digit+special
param openAiModelDeploymentName = 'Phi-4'
```

**Password Rules** (Azure SQL minimum):
- At least 12 characters
- At least one uppercase letter
- At least one lowercase letter
- At least one digit
- At least one special character (!@#$%^&*)

### Step 3: Deploy Azure Infrastructure

```bash
az login
az account set --subscription "<YOUR_SUBSCRIPTION_ID>"

cd infrastructure/scripts
chmod +x deploy.sh
./deploy.sh dev
```

This creates **13 Azure resources** in `biautomation-dev-rg`:

| Resource | Name | Purpose |
|----------|------|---------|
| Synapse Workspace + Dedicated Pool | `biautomation-dev-syn` / `bipool` (DW100c) | Medallion data warehouse |
| Function App (BI) | `biautomation-dev-func` | 48 endpoints + 3 timers |
| Function App (Test) | `biautomation-dev-test-func` | 11 test endpoints |
| App Service Plan | `biautomation-dev-asp` | Shared EP1 Premium plan |
| Azure SQL Database | `biautomation-dev-sourcedb` | Source data + Config DB |
| ADLS Gen2 Storage | `biautomationdevstorage` | Data lake + function storage |
| Key Vault | `biautomation-dev-kv` | Secrets + managed identity |
| Azure AI Foundry | `biautomation-dev-ai` | Phi-4 LLM for 15 agents |
| Data Factory | `biautomation-dev-adf` | Incremental load pipelines |
| App Insights | `biautomation-dev-insights` | Telemetry + monitoring |
| Log Analytics | `biautomation-dev-logs` | Centralized logging |
| Bot Service | `biautomation-dev-bot` | Teams integration (optional) |
| Container Registry | `biautomationdevacr` | Agent container images (future) |

**Deployment time**: 8-15 minutes.

### Step 4: Deploy Database Schema

```bash
# Get connection info
RG="biautomation-dev-rg"
SQL_SERVER=$(az sql server show --name biautomation-dev-sqlsrv --resource-group $RG --query fullyQualifiedDomainName -o tsv)
SYNAPSE_EP=$(az synapse workspace show --name biautomation-dev-syn --resource-group $RG --query connectivityEndpoints.sql -o tsv)

# Add your IP to SQL firewall
MY_IP=$(curl -s ifconfig.me)
az sql server firewall-rule create --resource-group $RG \
  --server biautomation-dev-sqlsrv \
  --name "Setup-$(date +%Y%m%d)" --start-ip-address $MY_IP --end-ip-address $MY_IP

# Create Synapse schemas + catalog tables
for f in catalog/ddl/01_create_schemas.sql \
         catalog/ddl/02_create_catalog_tables.sql \
         catalog/ddl/03_create_audit_tables.sql \
         catalog/ddl/03_create_config_tables.sql; do
  echo "Running $f..."
  sqlcmd -S "$SYNAPSE_EP" -d bipool -U sqladmin -P '<YOUR_PASSWORD>' -I -i "$f"
done

# Seed source data + catalog metadata
sqlcmd -S "$SQL_SERVER" -d biautomation-dev-sourcedb -U sqladmin -P '<YOUR_PASSWORD>' -I -i catalog/seed/01_seed_source_data.sql
sqlcmd -S "$SYNAPSE_EP" -d bipool -U sqladmin -P '<YOUR_PASSWORD>' -I -i catalog/seed/02_seed_catalog_metadata.sql
```

### Step 5: Configure Function App Settings

```bash
FUNC_APP="biautomation-dev-func"
RG="biautomation-dev-rg"

# Get deployment outputs
AI_ENDPOINT=$(az cognitiveservices account show --name biautomation-dev-ai --resource-group $RG --query properties.endpoint -o tsv)
AI_KEY=$(az cognitiveservices account keys list --name biautomation-dev-ai --resource-group $RG --query key1 -o tsv)
SQL_SERVER=$(az sql server show --name biautomation-dev-sqlsrv --resource-group $RG --query fullyQualifiedDomainName -o tsv)
SYNAPSE_EP=$(az synapse workspace show --name biautomation-dev-syn --resource-group $RG --query connectivityEndpoints.sql -o tsv)
STORAGE_NAME="biautomationdevstorage"
KV_URI="https://biautomation-dev-kv.vault.azure.net/"

# Set all required environment variables
az functionapp config appsettings set --name $FUNC_APP --resource-group $RG --settings \
  ENVIRONMENT=dev \
  KEY_VAULT_URI=$KV_URI \
  AZURE_OPENAI_ENDPOINT=$AI_ENDPOINT \
  AZURE_OPENAI_DEPLOYMENT=Phi-4 \
  AI_API_KEY=$AI_KEY \
  SYNAPSE_SQL_ENDPOINT=$SYNAPSE_EP \
  SYNAPSE_SQL_DATABASE=bipool \
  SQL_ADMIN_USER=sqladmin \
  SQL_ADMIN_PASSWORD='<YOUR_PASSWORD>' \
  SOURCE_DB_SERVER=$SQL_SERVER \
  SOURCE_DB_NAME=biautomation-dev-sourcedb \
  STORAGE_ACCOUNT_NAME=$STORAGE_NAME \
  CONFIG_DB_SERVER=$SQL_SERVER \
  CONFIG_DB_NAME=biautomation-dev-sourcedb
```

**Optional settings** (enable as needed):

```bash
# ADO integration
az functionapp config appsettings set --name $FUNC_APP --resource-group $RG --settings \
  ADO_ORG=YourOrgName \
  ADO_PROJECT=YourProject \
  ADO_REPO=YourRepo \
  ADO_PAT='<YOUR_PERSONAL_ACCESS_TOKEN>'

# Teams webhook
az functionapp config appsettings set --name $FUNC_APP --resource-group $RG --settings \
  TEAMS_WEBHOOK_URL='<INCOMING_WEBHOOK_URL>'
```

### Step 6: Deploy Function Apps

```bash
# Product 1: BI Pipeline
cd agents
pip install -r requirements.txt   # verify locally first
func azure functionapp publish biautomation-dev-func --python

# Product 2: Test Automation
cd ../test-automation
func azure functionapp publish biautomation-dev-test-func --python
```

### Step 7: Verify Deployment

```bash
FUNC_URL="https://biautomation-dev-func.azurewebsites.net"
FUNC_KEY=$(az functionapp keys list --name biautomation-dev-func --resource-group biautomation-dev-rg --query "functionKeys.default" -o tsv)

# Health check
curl "$FUNC_URL/api/health"

# Ops dashboard
curl "$FUNC_URL/api/ops/dashboard?code=$FUNC_KEY"

# Open Web UI
echo "Web UI: $FUNC_URL/api/ui"
```

### Step 8: Run Your First Pipeline

```bash
curl -X POST "$FUNC_URL/api/process-story?code=$FUNC_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "story_id": "STORY-001",
    "title": "Daily Sales Summary by Region",
    "business_objective": "Daily revenue by region and category",
    "source_system": "SalesDB",
    "source_tables": ["sales.OrderHeader", "sales.OrderDetail", "sales.Customer", "sales.Product"],
    "dimensions": ["Region", "ProductCategory", "OrderDate"],
    "metrics": ["SUM(LineTotal) AS TotalRevenue", "COUNT(DISTINCT OrderId) AS OrderCount"],
    "grain": "Daily, per Region, per ProductCategory",
    "acceptance_criteria": ["Row count > 0", "No null regions", "Revenue reconciles"]
  }'
```

---

## Environment Variables Reference

### Required (BI Pipeline)

| Variable | Description | Example |
|----------|-------------|---------|
| `KEY_VAULT_URI` | Key Vault endpoint | `https://biautomation-dev-kv.vault.azure.net/` |
| `AZURE_OPENAI_ENDPOINT` | AI Foundry endpoint | `https://biautomation-dev-ai.openai.azure.com/` |
| `AZURE_OPENAI_DEPLOYMENT` | Model deployment name | `Phi-4` |
| `AI_API_KEY` | AI Foundry API key | (from az cognitiveservices...) |
| `SYNAPSE_SQL_ENDPOINT` | Synapse SQL endpoint | `biautomation-dev-syn.sql.azuresynapse.net` |
| `SYNAPSE_SQL_DATABASE` | Dedicated pool name | `bipool` |
| `SQL_ADMIN_USER` | SQL admin username | `sqladmin` |
| `SQL_ADMIN_PASSWORD` | SQL admin password | (your strong password) |
| `SOURCE_DB_SERVER` | Source DB server | `biautomation-dev-sqlsrv.database.windows.net` |
| `SOURCE_DB_NAME` | Source database name | `biautomation-dev-sourcedb` |
| `STORAGE_ACCOUNT_NAME` | ADLS storage account | `biautomationdevstorage` |

### Optional

| Variable | Description | Default |
|----------|-------------|---------|
| `ENVIRONMENT` | Environment label | `dev` |
| `CONFIG_DB_SERVER` | Config DB (if separate) | Same as SOURCE_DB_SERVER |
| `CONFIG_DB_NAME` | Config DB name | Same as SOURCE_DB_NAME |
| `ADO_ORG` | Azure DevOps organization | (empty) |
| `ADO_PROJECT` | ADO project name | (empty) |
| `ADO_REPO` | ADO repo name | (empty) |
| `ADO_PAT` | ADO Personal Access Token | (empty) |
| `TEAMS_WEBHOOK_URL` | Teams Incoming Webhook URL | (empty) |
| `ADF_RESOURCE_GROUP` | Resource group for ADF | (empty) |
| `AZURE_SUBSCRIPTION_ID` | Sub ID for Synapse auto-pause | (empty) |

---

## Running Tests

```bash
cd synapse-bi-automation

# All 155 tests (no Azure needed)
python3 -m pytest tests/ -v

# With coverage
python3 -m pytest tests/ --cov=agents --cov-report=term-missing

# Single file
python3 -m pytest tests/unit/test_planner_agent.py -v
```

---

## Local Development

```bash
cd agents

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Create local.settings.json (copy and fill in values)
cat > local.settings.json << 'EOF'
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "ENVIRONMENT": "dev",
    "KEY_VAULT_URI": "https://biautomation-dev-kv.vault.azure.net/",
    "AZURE_OPENAI_ENDPOINT": "<YOUR_AI_ENDPOINT>",
    "AZURE_OPENAI_DEPLOYMENT": "Phi-4",
    "AI_API_KEY": "<YOUR_AI_KEY>",
    "SYNAPSE_SQL_ENDPOINT": "<YOUR_SYNAPSE_ENDPOINT>",
    "SYNAPSE_SQL_DATABASE": "bipool",
    "SQL_ADMIN_USER": "sqladmin",
    "SQL_ADMIN_PASSWORD": "<YOUR_PASSWORD>",
    "SOURCE_DB_SERVER": "<YOUR_SQL_SERVER>",
    "SOURCE_DB_NAME": "biautomation-dev-sourcedb",
    "STORAGE_ACCOUNT_NAME": "<YOUR_STORAGE>",
    "CONFIG_DB_SERVER": "<YOUR_SQL_SERVER>",
    "CONFIG_DB_NAME": "biautomation-dev-sourcedb"
  }
}
EOF

# Start locally (requires Azurite for storage emulation)
func start
```

The Web UI is available at `http://localhost:7071/api/ui`

---

## CI/CD with Azure DevOps

Two pipeline templates are provided in `pipelines/`:

| File | Trigger | What It Does |
|------|---------|-------------|
| `ci.yml` | Every push | Lint + unit tests |
| `cd-dev.yml` | Merge to develop | Bicep deploy -> Catalog DDL -> Function App (blue-green swap) -> Smoke test |

### Setup CI/CD:

1. In ADO, go to **Pipelines > New Pipeline > Azure Repos Git**
2. Select the repo, then **Existing Azure Pipelines YAML file**
3. Pick `pipelines/ci.yml` for CI, `pipelines/cd-dev.yml` for CD
4. Create a **Service Connection** named `biautomation-service-connection` (Azure Resource Manager, subscription scope)

---

## Estimated Monthly Cost

| Resource | SKU | Monthly Cost |
|----------|-----|-------------|
| Synapse Dedicated Pool | DW100c (auto-paused) | $150-300* |
| App Service Plan (EP1) | Premium Elastic | $150 |
| Azure SQL Database | Basic (5 DTU) | $5 |
| Azure AI Foundry | Phi-4 (pay-per-token) | $10-30 |
| ADLS Gen2 Storage | Hot, <1 GB | $1 |
| Key Vault | Standard | $1 |
| Data Factory | <10 pipeline runs/day | $5 |
| App Insights + Log Analytics | <5 GB/mo | $5 |
| **Total** | | **~$327-497/mo** |

*Synapse auto-pause reduces this significantly. With auto-pause enabled (30-min idle), expect ~$50-100/mo for light POC usage.

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `ConfigError: AZURE_OPENAI_ENDPOINT not set` | Missing Function App settings | Run Step 5 (configure settings) |
| Synapse pool paused | Auto-pause triggered or manual pause | Pipeline auto-resumes; or `POST /api/ops/pause-synapse` to toggle |
| ADO PAT expired | PAT has 90-day default expiry | Generate new PAT, update `ADO_PAT` setting |
| `pyodbc.OperationalError: Login timeout` | SQL firewall blocking | Add your IP: `az sql server firewall-rule create` |
| LLM returns garbage JSON | Model drift after update | Run `POST /api/ops/regression-test` to verify |
| `WEBSITE_CONTENTSHARE` error | Storage config issue | Set `WEBSITE_CONTENTSHARE` to function app name |
| Key Vault soft-delete conflict | Previous KV with same name | Purge: `az keyvault purge --name <name>` |
| Durable Task hub mismatch | Wrong hub name | BI uses `BiAutoHubV8`, Test uses `BiAutoTestHubV1` |

---

## Security Checklist

- [ ] SQL password meets complexity requirements and is stored in Key Vault
- [ ] ADO PAT has minimum required scopes (Work Items: Read+Write, Code: Read+Write)
- [ ] Function App uses Managed Identity for Key Vault access
- [ ] SQL firewall restricts access to known IPs only
- [ ] AI API key is in Function App settings (not in code)
- [ ] `local.settings.json` is in `.gitignore` (never commit secrets)
- [ ] Teams webhook URL is kept private (anyone with URL can post)
