# Lift-and-Shift Guide: POC to Production

## What Changes Between POC and Production

| Aspect | POC | Production | Change Required |
|--------|-----|------------|-----------------|
| Source | Azure SQL DB (simulated) | Azure SQL MI (real) | Update connection string + linked service |
| Synapse Pool | Serverless (built-in) | Dedicated SQL Pool | Change Bicep param, update DDL for distribution |
| Functions Plan | EP1 (or Consumption) | EP2/EP3 or Container Apps | Change Bicep SKU |
| Networking | Public endpoints | Private endpoints + VNet | Add Bicep VNet module |
| Auth | Azure AD (personal) | Managed Identity + SPN | Already using DefaultAzureCredential (no code change) |
| Key Vault | Open access | VNet-restricted | Update network ACLs in Bicep |
| OpenAI | 10K TPM | 60K+ TPM | Update Bicep capacity param |
| CI/CD | Manual / dev only | Full dev→qa→prod | Add cd-qa.yml, cd-prod.yml |
| Approval | None | Azure DevOps gates | Add environment approval in pipelines |
| Monitoring | Basic App Insights | Full alerting + dashboards | Add alert rules in Bicep |

## Step-by-Step Lift-and-Shift

### 1. Create Production Parameter File
```
Already exists: infrastructure/bicep/parameters/prod.bicepparam
Update with production values:
  - prefix: same or different
  - synapseSqlAdminObjectId: prod AD group object ID
  - SQL MI connection details in Key Vault
```

### 2. Deploy to Production Resource Group
```bash
./infrastructure/scripts/deploy.sh prod biautomation-prod-rg
```
Same Bicep templates, different parameters. Zero code changes.

### 3. Update Source Connection
Replace simulated Azure SQL DB with real Azure SQL MI:
```bash
az keyvault secret set --vault-name biautomation-prod-kv \
  --name "source-db-connection-string" \
  --value "Server=tcp:<your-sqlmi>.database.windows.net,1433;Database=<real-db>;..."
```

Update `catalog.source_systems` to point to real SQL MI.

### 4. Re-seed Catalog with Real Metadata
Run the Planner Agent's source discovery against real SQL MI to populate:
- `catalog.source_tables` (real columns, types, keys)
- `catalog.approved_joins` (validated by your team)
- `catalog.business_glossary` (real business terms)

### 5. Add VNet + Private Endpoints (Production Security)
Add a new Bicep module `infrastructure/bicep/modules/networking.bicep` with:
- VNet + subnets
- Private endpoints for Synapse, Key Vault, Storage, SQL MI
- NSG rules
- VNet integration for Function App

### 6. Add Production CI/CD Pipelines
Copy `pipelines/cd-dev.yml` → `pipelines/cd-prod.yml`
Changes:
- Trigger on `main` branch
- Add environment approval gate
- Use production service connection
- Use production resource group and parameters

### 7. What Does NOT Change
- All agent Python code (zero changes)
- All prompt templates (zero changes)
- All SQL templates (zero changes)
- Orchestrator logic (zero changes)
- Data models / contracts (zero changes)
- Validation rules (zero changes)

The entire application is environment-agnostic by design.
