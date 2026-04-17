<#
.SYNOPSIS
    One-click deployment of the BI Automation Platform to any Azure subscription.

.DESCRIPTION
    Deploys all infrastructure (Bicep), seeds databases, configures app settings,
    deploys the Function App code, and verifies the health endpoint.

    Prerequisites:
    - Azure CLI installed (az --version)
    - Azure Functions Core Tools installed (func --version)
    - PowerShell 7+ recommended
    - Logged into Azure (az login)

.EXAMPLE
    # Deploy to a new subscription (dev environment)
    .\deploy.ps1 -Environment dev -Prefix biautomation -Location westus2

    # Deploy to production
    .\deploy.ps1 -Environment prod -Prefix biautoprod -Location eastus2

.NOTES
    Estimated time: 15-25 minutes (first deploy) / 5-10 minutes (subsequent)
    Estimated cost: ~$5-10/day when Synapse pool is running, ~$1/day when paused
#>

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("dev", "qa", "prod")]
    [string]$Environment = "dev",

    [Parameter(Mandatory=$false)]
    [string]$Prefix = "biautomation",

    [Parameter(Mandatory=$false)]
    [string]$Location = "westus2",

    [Parameter(Mandatory=$false)]
    [string]$SqlAdminUser = "sqladmin",

    [Parameter(Mandatory=$false)]
    [SecureString]$SqlAdminPassword,

    [Parameter(Mandatory=$false)]
    [string]$AiEndpoint = "",

    [Parameter(Mandatory=$false)]
    [string]$AiDeploymentName = "Phi-4",

    [Parameter(Mandatory=$false)]
    [string]$AiApiKey = "",

    [Parameter(Mandatory=$false)]
    [string]$AdoOrgUrl = "",

    [Parameter(Mandatory=$false)]
    [string]$AdoProject = "",

    [Parameter(Mandatory=$false)]
    [string]$AdoPat = "",

    [Parameter(Mandatory=$false)]
    [string]$TeamsWebhookUrl = "",

    [Parameter(Mandatory=$false)]
    [switch]$SkipInfra,

    [Parameter(Mandatory=$false)]
    [switch]$SkipSql,

    [Parameter(Mandatory=$false)]
    [switch]$SkipFuncDeploy,

    [Parameter(Mandatory=$false)]
    [switch]$SkipVerify,

    [Parameter(Mandatory=$false)]
    [switch]$PauseSynapseAfterDeploy
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path "$ScriptDir/../..").Path
$BicepDir = "$RepoRoot/infrastructure/bicep"
$SqlDir = "$RepoRoot/infrastructure/sql"
$CatalogDdlDir = "$RepoRoot/catalog/ddl"
$CatalogSeedDir = "$RepoRoot/catalog/seed"
$AgentsDir = "$RepoRoot/agents"

# ============================================================
# NAMING CONVENTION
# ============================================================
$ResourceGroup = "rg-$Prefix-$Environment"
$FuncAppName = "$Prefix-$Environment-func"
$SqlServerName = "$Prefix-$Environment-sqlsrv"
$SqlDbName = "$Prefix-$Environment-sourcedb"
$SynapseWorkspace = "$Prefix-$Environment-syn"
$SynapsePool = "bipool"

Write-Host ""
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host "  BI AUTOMATION PLATFORM - DEPLOYMENT" -ForegroundColor Yellow
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host "  Environment:      $Environment"
Write-Host "  Prefix:           $Prefix"
Write-Host "  Location:         $Location"
Write-Host "  Resource Group:   $ResourceGroup"
Write-Host "  Function App:     $FuncAppName"
Write-Host "  SQL Server:       $SqlServerName"
Write-Host "  Synapse:          $SynapseWorkspace"
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host ""

# ============================================================
# STEP 0: Pre-flight checks
# ============================================================
Write-Host "[0/8] Pre-flight checks..." -ForegroundColor Cyan

# Azure CLI
try {
    $azVersion = az version 2>$null | ConvertFrom-Json
    Write-Host "  Azure CLI: $($azVersion.'azure-cli')" -ForegroundColor Green
} catch {
    Write-Host "  ERROR: Azure CLI not found. Install from https://aka.ms/installazurecli" -ForegroundColor Red
    exit 1
}

# Logged in?
try {
    $account = az account show 2>$null | ConvertFrom-Json
    Write-Host "  Subscription: $($account.name) ($($account.id))" -ForegroundColor Green
    Write-Host "  User: $($account.user.name)" -ForegroundColor Green
} catch {
    Write-Host "  ERROR: Not logged in. Run 'az login' first." -ForegroundColor Red
    exit 1
}

# Functions Core Tools
try {
    $funcVer = func --version 2>$null
    Write-Host "  Functions Core Tools: $funcVer" -ForegroundColor Green
} catch {
    Write-Host "  WARNING: Azure Functions Core Tools not found. Install from https://aka.ms/azfunc-install" -ForegroundColor Yellow
    Write-Host "  (Function App code deploy will be skipped)" -ForegroundColor Yellow
    $SkipFuncDeploy = $true
}

# SQL password
if (-not $SqlAdminPassword) {
    $SqlAdminPassword = Read-Host "Enter SQL admin password (min 12 chars, upper+lower+number+special)" -AsSecureString
}
$SqlPasswordPlain = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($SqlAdminPassword)
)

# Validate password complexity
if ($SqlPasswordPlain.Length -lt 12) {
    Write-Host "  ERROR: Password must be at least 12 characters" -ForegroundColor Red
    exit 1
}

Write-Host "  All pre-flight checks passed" -ForegroundColor Green
Write-Host ""

# ============================================================
# STEP 1: Register resource providers
# ============================================================
Write-Host "[1/8] Registering resource providers..." -ForegroundColor Cyan

$providers = @(
    "Microsoft.Web",
    "Microsoft.Sql",
    "Microsoft.Synapse",
    "Microsoft.DataFactory",
    "Microsoft.CognitiveServices",
    "Microsoft.KeyVault",
    "Microsoft.Storage",
    "Microsoft.OperationalInsights",
    "Microsoft.Insights"
)

foreach ($provider in $providers) {
    $state = (az provider show --namespace $provider --query "registrationState" -o tsv 2>$null)
    if ($state -ne "Registered") {
        Write-Host "  Registering $provider..." -ForegroundColor Yellow
        az provider register --namespace $provider --wait 2>$null
    } else {
        Write-Host "  $provider already registered" -ForegroundColor DarkGray
    }
}
Write-Host ""

# ============================================================
# STEP 2: Create resource group
# ============================================================
Write-Host "[2/8] Creating resource group '$ResourceGroup'..." -ForegroundColor Cyan

az group create `
    --name $ResourceGroup `
    --location $Location `
    --tags project=bi-automation environment=$Environment managed-by=deploy-script `
    --output none

Write-Host "  Resource group ready" -ForegroundColor Green
Write-Host ""

# ============================================================
# STEP 3: Deploy infrastructure (Bicep)
# ============================================================
if (-not $SkipInfra) {
    Write-Host "[3/8] Deploying infrastructure (Bicep)... (~10-15 minutes)" -ForegroundColor Cyan

    # Get AAD object ID for Synapse admin
    $ObjectId = az ad signed-in-user show --query id -o tsv 2>$null
    if (-not $ObjectId) {
        Write-Host "  WARNING: Could not get AAD object ID. Using placeholder." -ForegroundColor Yellow
        $ObjectId = "00000000-0000-0000-0000-000000000000"
    }
    Write-Host "  AAD Object ID: $ObjectId" -ForegroundColor DarkGray

    $deploymentName = "bi-auto-$Environment-$(Get-Date -Format 'yyyyMMddHHmm')"

    az deployment group create `
        --resource-group $ResourceGroup `
        --template-file "$BicepDir/main.bicep" `
        --parameters environment=$Environment `
        --parameters prefix=$Prefix `
        --parameters synapseSqlAdminObjectId=$ObjectId `
        --parameters sqlAdminUsername=$SqlAdminUser `
        --parameters sqlAdminPassword=$SqlPasswordPlain `
        --parameters sqlLocation=$Location `
        --name $deploymentName `
        --output none

    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ERROR: Bicep deployment failed. Check Azure portal for details." -ForegroundColor Red
        exit 1
    }

    # Get outputs
    $outputs = az deployment group show `
        --resource-group $ResourceGroup `
        --name $deploymentName `
        --query "properties.outputs" | ConvertFrom-Json

    $SynapseSqlEndpoint = $outputs.synapseSqlEndpoint.value
    $SqlServerFqdn = $outputs.sqlServerFqdn.value

    Write-Host "  Infrastructure deployed successfully" -ForegroundColor Green
    Write-Host "  Synapse endpoint: $SynapseSqlEndpoint" -ForegroundColor DarkGray
    Write-Host "  SQL Server: $SqlServerFqdn" -ForegroundColor DarkGray
} else {
    Write-Host "[3/8] Skipping infrastructure (--SkipInfra)" -ForegroundColor DarkGray
    $SqlServerFqdn = "$SqlServerName.database.windows.net"
    $SynapseSqlEndpoint = "$SynapseWorkspace.sql.azuresynapse.net"
}
Write-Host ""

# ============================================================
# STEP 4: Run SQL schema scripts
# ============================================================
if (-not $SkipSql) {
    Write-Host "[4/8] Running SQL schema scripts..." -ForegroundColor Cyan

    # Check if sqlcmd is available
    $hasSqlCmd = $null -ne (Get-Command sqlcmd -ErrorAction SilentlyContinue)

    if ($hasSqlCmd) {
        # Against Azure SQL (Config DB + Source data)
        $sqlConn = "Server=tcp:$SqlServerFqdn,1433;Database=$SqlDbName;User ID=$SqlAdminUser;Password=$SqlPasswordPlain;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"

        $sqlScripts = @(
            @{ Path = "$CatalogDdlDir/01_create_schemas.sql"; Desc = "Schemas (bronze/silver/gold/catalog/audit)" },
            @{ Path = "$CatalogDdlDir/02_create_catalog_tables.sql"; Desc = "Catalog tables" },
            @{ Path = "$CatalogDdlDir/03_create_config_tables.sql"; Desc = "Config tables (pipeline_registry, state)" },
            @{ Path = "$CatalogDdlDir/03_create_audit_tables.sql"; Desc = "Audit tables" },
            @{ Path = "$SqlDir/test_schema.sql"; Desc = "Test automation tables" },
            @{ Path = "$CatalogSeedDir/01_seed_source_data.sql"; Desc = "Sample source data (435 records)" }
        )

        foreach ($script in $sqlScripts) {
            if (Test-Path $script.Path) {
                Write-Host "  Running: $($script.Desc)..." -ForegroundColor DarkGray
                sqlcmd -S "tcp:$SqlServerFqdn,1433" -d $SqlDbName -U $SqlAdminUser -P $SqlPasswordPlain -i $script.Path -b 2>$null
                if ($LASTEXITCODE -ne 0) {
                    Write-Host "    WARNING: Script had errors (may be OK if tables already exist)" -ForegroundColor Yellow
                }
            } else {
                Write-Host "  SKIP: $($script.Path) not found" -ForegroundColor DarkGray
            }
        }

        # Against Synapse (medallion schemas)
        Write-Host "  Running schemas on Synapse..." -ForegroundColor DarkGray
        $synapseSchemaScript = "$CatalogDdlDir/01_create_schemas.sql"
        if (Test-Path $synapseSchemaScript) {
            sqlcmd -S "tcp:$SynapseSqlEndpoint,1433" -d $SynapsePool -U $SqlAdminUser -P $SqlPasswordPlain -i $synapseSchemaScript -b 2>$null
            if ($LASTEXITCODE -ne 0) {
                Write-Host "    WARNING: Synapse schemas may need pool to be running" -ForegroundColor Yellow
            }
        }

        Write-Host "  SQL scripts complete" -ForegroundColor Green
    } else {
        Write-Host "  WARNING: sqlcmd not found. Run SQL scripts manually:" -ForegroundColor Yellow
        Write-Host "    Install: https://learn.microsoft.com/en-us/sql/tools/sqlcmd/sqlcmd-utility" -ForegroundColor DarkGray
        Write-Host "    Then run:" -ForegroundColor DarkGray
        Write-Host "      sqlcmd -S tcp:$SqlServerFqdn,1433 -d $SqlDbName -U $SqlAdminUser -P <password> -i catalog/ddl/01_create_schemas.sql" -ForegroundColor DarkGray
        Write-Host "      sqlcmd -S tcp:$SqlServerFqdn,1433 -d $SqlDbName -U $SqlAdminUser -P <password> -i catalog/ddl/02_create_catalog_tables.sql" -ForegroundColor DarkGray
        Write-Host "      sqlcmd -S tcp:$SqlServerFqdn,1433 -d $SqlDbName -U $SqlAdminUser -P <password> -i catalog/ddl/03_create_config_tables.sql" -ForegroundColor DarkGray
        Write-Host "      sqlcmd -S tcp:$SqlServerFqdn,1433 -d $SqlDbName -U $SqlAdminUser -P <password> -i catalog/ddl/03_create_audit_tables.sql" -ForegroundColor DarkGray
        Write-Host "      sqlcmd -S tcp:$SqlServerFqdn,1433 -d $SqlDbName -U $SqlAdminUser -P <password> -i infrastructure/sql/test_schema.sql" -ForegroundColor DarkGray
        Write-Host "      sqlcmd -S tcp:$SqlServerFqdn,1433 -d $SqlDbName -U $SqlAdminUser -P <password> -i catalog/seed/01_seed_source_data.sql" -ForegroundColor DarkGray
    }
} else {
    Write-Host "[4/8] Skipping SQL scripts (--SkipSql)" -ForegroundColor DarkGray
}
Write-Host ""

# ============================================================
# STEP 5: Configure Function App settings
# ============================================================
Write-Host "[5/8] Configuring Function App settings..." -ForegroundColor Cyan

$settings = @(
    "SYNAPSE_SQL_ENDPOINT=$SynapseSqlEndpoint",
    "SYNAPSE_SQL_DATABASE=$SynapsePool",
    "SQL_ADMIN_USER=$SqlAdminUser",
    "SQL_ADMIN_PASSWORD=$SqlPasswordPlain",
    "SOURCE_DB_SERVER=$SqlServerFqdn",
    "SOURCE_DB_NAME=$SqlDbName"
)

# AI settings (if provided)
if ($AiEndpoint) {
    $settings += "AI_ENDPOINT=$AiEndpoint"
    $settings += "AI_DEPLOYMENT_NAME=$AiDeploymentName"
}
if ($AiApiKey) {
    $settings += "AI_API_KEY=$AiApiKey"
}

# ADO settings (if provided)
if ($AdoOrgUrl) {
    $settings += "ADO_ORG_URL=$AdoOrgUrl"
    $settings += "ADO_PROJECT=$AdoProject"
    $settings += "ADO_PAT=$AdoPat"
}

# Teams webhook (if provided)
if ($TeamsWebhookUrl) {
    $settings += "TEAMS_WEBHOOK_URL=$TeamsWebhookUrl"
}

$settingsStr = $settings -join " "
az functionapp config appsettings set `
    --name $FuncAppName `
    --resource-group $ResourceGroup `
    --settings $settings `
    --output none 2>$null

if ($LASTEXITCODE -ne 0) {
    Write-Host "  WARNING: Could not set app settings (Function App may not exist yet)" -ForegroundColor Yellow
} else {
    Write-Host "  App settings configured ($($settings.Count) settings)" -ForegroundColor Green
}
Write-Host ""

# ============================================================
# STEP 6: Deploy Function App code
# ============================================================
if (-not $SkipFuncDeploy) {
    Write-Host "[6/8] Deploying Function App code..." -ForegroundColor Cyan

    Push-Location $AgentsDir
    try {
        func azure functionapp publish $FuncAppName --python 2>&1 | ForEach-Object {
            if ($_ -match "error|Error|ERROR") {
                Write-Host "  $_" -ForegroundColor Red
            } elseif ($_ -match "Remote build succeeded|Deployment successful") {
                Write-Host "  $_" -ForegroundColor Green
            }
        }
        Write-Host "  Function App deployed" -ForegroundColor Green
    } catch {
        Write-Host "  ERROR: Function App deployment failed: $_" -ForegroundColor Red
    } finally {
        Pop-Location
    }
} else {
    Write-Host "[6/8] Skipping Function App deploy (--SkipFuncDeploy)" -ForegroundColor DarkGray
}
Write-Host ""

# ============================================================
# STEP 7: Resume Synapse pool (if paused)
# ============================================================
Write-Host "[7/8] Checking Synapse pool status..." -ForegroundColor Cyan

try {
    $poolStatus = az synapse sql pool show `
        --workspace-name $SynapseWorkspace `
        --name $SynapsePool `
        --resource-group $ResourceGroup `
        --query "status" -o tsv 2>$null

    if ($poolStatus -eq "Paused") {
        if ($PauseSynapseAfterDeploy) {
            Write-Host "  Pool is paused (will stay paused as requested)" -ForegroundColor DarkGray
        } else {
            Write-Host "  Resuming Synapse pool (takes ~2 minutes)..." -ForegroundColor Yellow
            az synapse sql pool resume `
                --workspace-name $SynapseWorkspace `
                --name $SynapsePool `
                --resource-group $ResourceGroup `
                --output none 2>$null
            Write-Host "  Synapse pool resumed" -ForegroundColor Green
        }
    } elseif ($poolStatus -eq "Online") {
        Write-Host "  Synapse pool already running" -ForegroundColor Green
    } else {
        Write-Host "  Synapse pool status: $poolStatus" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  WARNING: Could not check Synapse pool (may not be deployed yet)" -ForegroundColor Yellow
}
Write-Host ""

# ============================================================
# STEP 8: Verify deployment
# ============================================================
if (-not $SkipVerify) {
    Write-Host "[8/8] Verifying deployment..." -ForegroundColor Cyan

    # Get function key
    try {
        $funcKey = az functionapp keys list `
            --name $FuncAppName `
            --resource-group $ResourceGroup `
            --query "functionKeys.default" -o tsv 2>$null

        if ($funcKey) {
            $healthUrl = "https://$FuncAppName.azurewebsites.net/api/health?code=$funcKey"
            Write-Host "  Checking health endpoint..." -ForegroundColor DarkGray

            try {
                $response = Invoke-RestMethod -Uri $healthUrl -TimeoutSec 30 -ErrorAction Stop
                Write-Host "  Health check: PASSED" -ForegroundColor Green
            } catch {
                Write-Host "  Health check: endpoint not responding yet (may need a few minutes to warm up)" -ForegroundColor Yellow
            }

            $webUiUrl = "https://$FuncAppName.azurewebsites.net/static/index.html"
            Write-Host ""
            Write-Host "  Web UI: $webUiUrl" -ForegroundColor Cyan
            Write-Host "  Function Key: $funcKey" -ForegroundColor DarkGray
        } else {
            Write-Host "  Could not retrieve function key" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "  WARNING: Could not verify (Function App may still be starting)" -ForegroundColor Yellow
    }
} else {
    Write-Host "[8/8] Skipping verification (--SkipVerify)" -ForegroundColor DarkGray
}

# ============================================================
# SUMMARY
# ============================================================
Write-Host ""
Write-Host "================================================================" -ForegroundColor Green
Write-Host "  DEPLOYMENT COMPLETE" -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Resources:" -ForegroundColor White
Write-Host "    Resource Group:   $ResourceGroup"
Write-Host "    Function App:     https://$FuncAppName.azurewebsites.net"
Write-Host "    Web UI:           https://$FuncAppName.azurewebsites.net/static/index.html"
Write-Host "    SQL Server:       $SqlServerFqdn"
Write-Host "    Synapse:          $SynapseSqlEndpoint"
Write-Host ""
Write-Host "  Post-deploy checklist:" -ForegroundColor Yellow
Write-Host "    [ ] Set AI_ENDPOINT + AI_API_KEY if not provided"
Write-Host "    [ ] Set ADO_ORG_URL + ADO_PROJECT + ADO_PAT if not provided"
Write-Host "    [ ] Set TEAMS_WEBHOOK_URL if not provided"
Write-Host "    [ ] Resume Synapse pool before running pipelines"
Write-Host "    [ ] Install local test agent: pip install -e agents/tester/local-agent"
Write-Host ""
Write-Host "  Cost management:" -ForegroundColor Yellow
Write-Host "    Pause Synapse when not in use:"
Write-Host "      az synapse sql pool pause --workspace-name $SynapseWorkspace --name $SynapsePool --resource-group $ResourceGroup"
Write-Host "    Resume when needed:"
Write-Host "      az synapse sql pool resume --workspace-name $SynapseWorkspace --name $SynapsePool --resource-group $ResourceGroup"
Write-Host ""
Write-Host "  Quick test:" -ForegroundColor Yellow
Write-Host "    Open Web UI and enter ADO Work Item ID to run the BI pipeline."
Write-Host ""
Write-Host "================================================================" -ForegroundColor Green
