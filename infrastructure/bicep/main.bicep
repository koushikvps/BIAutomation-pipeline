// ============================================================
// MAIN DEPLOYMENT: Synapse BI Automation Framework
// Deploys all Azure resources
// Parameterized for lift-and-shift to production
// ============================================================

targetScope = 'resourceGroup'

@description('Environment name (dev, qa, prod)')
@allowed(['dev', 'qa', 'prod'])
param environment string = 'dev'

@description('Resource name prefix')
param prefix string = 'biautomation'

@description('Azure region')
param location string = resourceGroup().location

@description('Azure AD admin object ID for Synapse')
param synapseSqlAdminObjectId string

@description('SQL admin username for simulated source DB')
@secure()
param sqlAdminUsername string

@description('SQL admin password for simulated source DB')
@secure()
param sqlAdminPassword string

@description('Azure OpenAI model deployment name')
param openAiModelDeploymentName string = 'gpt-4o'

@description('Deploy Azure OpenAI (requires separate approval - set false to skip)')
param deployOpenAi bool = false

@description('Fallback region for SQL Server if primary region has capacity issues')
param sqlLocation string = 'westus2'

// ============================================================
// NAMING CONVENTION (lift-and-shift: change prefix + env)
// ============================================================
var baseName = '${prefix}-${environment}'
var uniqueSuffix = uniqueString(resourceGroup().id)
var storageAccountName = replace('${prefix}${environment}${uniqueSuffix}', '-', '')
var keyVaultName = '${baseName}-kv'
var synapseWorkspaceName = '${baseName}-syn'
var functionAppName = '${baseName}-func'
var testFunctionAppName = '${baseName}-test-func'
var appInsightsName = '${baseName}-ai'
var logAnalyticsName = '${baseName}-log'
var openAiName = '${baseName}-oai'
var sqlServerName = '${baseName}-sqlsrv'
var sqlDbName = '${baseName}-sourcedb'
var appServicePlanName = '${baseName}-asp'
var searchServiceName = '${baseName}-search'

// ============================================================
// MODULE: Storage Account (ADLS Gen2)
// ============================================================
module storage 'modules/storage.bicep' = {
  name: 'storage-deployment'
  params: {
    storageAccountName: take(storageAccountName, 24)
    location: location
    environment: environment
  }
}

// ============================================================
// MODULE: Log Analytics + Application Insights
// (deployed early — Key Vault diagnostics needs Log Analytics ID)
// ============================================================
module monitoring 'modules/monitoring.bicep' = {
  name: 'monitoring-deployment'
  params: {
    logAnalyticsName: logAnalyticsName
    appInsightsName: appInsightsName
    location: location
    environment: environment
  }
}

// ============================================================
// MODULE: Key Vault (deployed before Function App - no RBAC yet)
// ============================================================
module keyVault 'modules/keyvault.bicep' = {
  name: 'keyvault-deployment'
  params: {
    keyVaultName: keyVaultName
    location: location
    logAnalyticsWorkspaceId: monitoring.outputs.logAnalyticsId
    environment: environment
  }
  dependsOn: [monitoring]
}

// ============================================================
// MODULE: Azure Synapse Analytics
// ============================================================
module synapse 'modules/synapse.bicep' = {
  name: 'synapse-deployment'
  params: {
    workspaceName: synapseWorkspaceName
    location: location
    storageAccountId: storage.outputs.storageAccountId
    storageAccountUrl: storage.outputs.dfsEndpoint
    sqlAdminObjectId: synapseSqlAdminObjectId
    sqlAdminPassword: sqlAdminPassword
    environment: environment
  }
}

// ============================================================
// MODULE: Azure SQL Database (Simulated Source)
// ============================================================
module sqlDb 'modules/sql-source.bicep' = {
  name: 'sql-source-deployment'
  params: {
    sqlServerName: sqlServerName
    sqlDbName: sqlDbName
    location: sqlLocation
    adminUsername: sqlAdminUsername
    adminPassword: sqlAdminPassword
    aadAdminObjectId: synapseSqlAdminObjectId
    aadAdminLogin: 'SQL AAD Admin'
    environment: environment
    logAnalyticsWorkspaceId: monitoring.outputs.logAnalyticsId
  }
  dependsOn: [monitoring]
}

// ============================================================
// MODULE: Azure OpenAI (optional - requires approval)
// ============================================================
module openAi 'modules/openai.bicep' = if (deployOpenAi) {
  name: 'openai-deployment'
  params: {
    openAiName: openAiName
    location: location
    modelDeploymentName: openAiModelDeploymentName
    environment: environment
  }
}

// ============================================================
// MODULE: Azure AI Search (RAG Knowledge Base)
// ============================================================
module search 'modules/search.bicep' = {
  name: 'search-deployment'
  params: {
    searchServiceName: searchServiceName
    location: location
    environment: environment
  }
}

// ============================================================
// MODULE: Azure Data Factory
// ============================================================
var dataFactoryName = '${baseName}-adf'

module dataFactory 'modules/data-factory.bicep' = {
  name: 'data-factory-deployment'
  params: {
    dataFactoryName: dataFactoryName
    location: location
    sourceSqlServerFqdn: sqlDb.outputs.serverFqdn
    sourceSqlDbName: sqlDbName
    sqlAdminUsername: sqlAdminUsername
    storageAccountName: take(storageAccountName, 24)
    keyVaultName: keyVaultName
    keyVaultUri: keyVault.outputs.keyVaultUri
    environment: environment
  }
  dependsOn: [storage, sqlDb, keyVault]
}

// NOTE: monitoring module is deployed earlier (before Key Vault) so that
// Log Analytics workspace ID is available for Key Vault diagnostics.

// ============================================================
// MODULE: Key Vault Secrets (store all secrets before Function App)
// ============================================================
module keyVaultSecrets 'modules/keyvault-secrets.bicep' = {
  name: 'keyvault-secrets-deployment'
  params: {
    keyVaultName: keyVaultName
    sqlAdminPassword: sqlAdminPassword
  }
  dependsOn: [keyVault]
}

// ============================================================
// MODULE: Azure Functions (Agent Runtime)
// All secrets via Key Vault references — no plaintext passwords
// ============================================================
module functionApp 'modules/function-app.bicep' = {
  name: 'function-app-deployment'
  params: {
    functionAppName: functionAppName
    appServicePlanName: appServicePlanName
    location: location
    storageAccountName: take(storageAccountName, 24)
    appInsightsConnectionString: monitoring.outputs.appInsightsConnectionString
    keyVaultName: keyVaultName
    keyVaultUri: keyVault.outputs.keyVaultUri
    openAiEndpoint: deployOpenAi ? openAi.outputs.endpoint : ''
    synapseEndpoint: synapse.outputs.sqlPoolEndpoint
    synapseSqlUser: synapse.outputs.sqlAdministratorLogin
    synapseDatabase: synapse.outputs.sqlPoolDatabaseName
    environment: environment
    adfName: dataFactoryName
    searchEndpoint: search.outputs.searchEndpoint
  }
  dependsOn: [keyVaultSecrets, search]
}

// ============================================================
// MODULE: Test Automation Function App (separate product)
// Shares App Service Plan with BI Function App
// ============================================================
module testFunctionApp 'modules/function-app-test.bicep' = {
  name: 'test-function-app-deployment'
  params: {
    functionAppName: testFunctionAppName
    appServicePlanId: functionApp.outputs.appServicePlanId
    location: location
    storageAccountName: take(storageAccountName, 24)
    appInsightsConnectionString: monitoring.outputs.appInsightsConnectionString
    keyVaultName: keyVaultName
    keyVaultUri: keyVault.outputs.keyVaultUri
    openAiEndpoint: deployOpenAi ? openAi.outputs.endpoint : ''
    synapseEndpoint: synapse.outputs.sqlPoolEndpoint
    synapseSqlUser: synapse.outputs.sqlAdministratorLogin
    synapseDatabase: synapse.outputs.sqlPoolDatabaseName
    sourceSqlServerFqdn: sqlDb.outputs.serverFqdn
    sourceSqlDbName: sqlDbName
    environment: environment
  }
  dependsOn: [keyVaultSecrets, functionApp]
}

// ============================================================
// RBAC: Key Vault access for Function App + Test App + Synapse
// (deployed after all are created to break circular dependency)
// ============================================================
module keyVaultRbac 'modules/keyvault-rbac.bicep' = {
  name: 'keyvault-rbac-deployment'
  params: {
    keyVaultName: keyVaultName
    functionAppPrincipalId: functionApp.outputs.principalId
    testFunctionAppPrincipalId: testFunctionApp.outputs.principalId
    synapsePrincipalId: synapse.outputs.principalId
  }
  dependsOn: [keyVault, functionApp, testFunctionApp, synapse]
}

// ============================================================
// RBAC: Storage access for ADF + Function Apps + Synapse
// Required because storage has allowSharedKeyAccess: false
// ============================================================
module storageRbac 'modules/storage-rbac.bicep' = {
  name: 'storage-rbac-deployment'
  params: {
    storageAccountName: take(storageAccountName, 24)
    adfPrincipalId: dataFactory.outputs.dataFactoryPrincipalId
    functionAppPrincipalId: functionApp.outputs.principalId
    testFunctionAppPrincipalId: testFunctionApp.outputs.principalId
    synapsePrincipalId: synapse.outputs.principalId
  }
  dependsOn: [storage, dataFactory, functionApp, testFunctionApp, synapse]
}

// ============================================================
// RBAC: Azure AI Search access for Function Apps
// Search Index Data Contributor (read/write index documents)
// ============================================================
var searchIndexDataContributorRole = '8bbe4f35-0f5b-4a65-a0a3-fba521b05e44'

resource searchRbacFunc 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(search.outputs.searchServiceId, functionApp.outputs.principalId, searchIndexDataContributorRole)
  scope: resourceGroup()
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', searchIndexDataContributorRole)
    principalId: functionApp.outputs.principalId
    principalType: 'ServicePrincipal'
  }
  dependsOn: [search, functionApp]
}

resource searchRbacTestFunc 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(search.outputs.searchServiceId, testFunctionApp.outputs.principalId, searchIndexDataContributorRole)
  scope: resourceGroup()
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', searchIndexDataContributorRole)
    principalId: testFunctionApp.outputs.principalId
    principalType: 'ServicePrincipal'
  }
  dependsOn: [search, testFunctionApp]
}

// Search Service Contributor (create/manage indexes)
var searchServiceContributorRole = '7ca78c08-252a-4471-8644-bb5ff32d4ba0'

resource searchMgmtRbacFunc 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(search.outputs.searchServiceId, functionApp.outputs.principalId, searchServiceContributorRole)
  scope: resourceGroup()
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', searchServiceContributorRole)
    principalId: functionApp.outputs.principalId
    principalType: 'ServicePrincipal'
  }
  dependsOn: [search, functionApp]
}

// ============================================================
// OUTPUTS (used by CI/CD and agents)
// Non-sensitive resource names and endpoints only.
// Sensitive values (connection strings, keys) are stored in Key Vault.
// ============================================================
output storageAccountName string = take(storageAccountName, 24)
output synapseWorkspaceName string = synapseWorkspaceName
output synapseSqlEndpoint string = synapse.outputs.sqlEndpoint
output functionAppName string = functionAppName
output keyVaultName string = keyVaultName
output keyVaultUri string = keyVault.outputs.keyVaultUri
output openAiEndpoint string = deployOpenAi ? openAi.outputs.endpoint : 'NOT_DEPLOYED'
output sqlServerFqdn string = sqlDb.outputs.serverFqdn
output dataFactoryName string = dataFactoryName
output testFunctionAppName string = testFunctionAppName
output logAnalyticsWorkspaceId string = monitoring.outputs.logAnalyticsId
output searchEndpoint string = search.outputs.searchEndpoint
output searchServiceName string = searchServiceName
