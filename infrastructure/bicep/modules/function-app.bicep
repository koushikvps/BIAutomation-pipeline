// Azure Functions (Premium Plan) - Agent Runtime
// All secrets via Key Vault references, managed identity for storage

@description('Function App name')
param functionAppName string

@description('App Service Plan name')
param appServicePlanName string

@description('Azure region')
param location string

@description('Storage account name for Function App')
param storageAccountName string

@description('Application Insights connection string')
param appInsightsConnectionString string

@description('Key Vault name')
param keyVaultName string

@description('Key Vault URI')
param keyVaultUri string

@description('Azure OpenAI endpoint')
param openAiEndpoint string

@description('Synapse SQL endpoint')
param synapseEndpoint string

@description('Synapse SQL admin user')
param synapseSqlUser string

@description('Synapse dedicated pool database name')
param synapseDatabase string

@description('Environment')
param environment string

@description('Azure Data Factory name')
param adfName string = ''

@description('Azure AI Search endpoint for RAG knowledge base')
param searchEndpoint string = ''

@description('CORS allowed origins')
param corsAllowedOrigins array = ['https://portal.azure.com']

resource appServicePlan 'Microsoft.Web/serverfarms@2023-01-01' = {
  name: appServicePlanName
  location: location
  kind: 'elastic'
  sku: {
    name: 'EP1'
    tier: 'ElasticPremium'
    size: 'EP1'
    family: 'EP'
    capacity: 1
  }
  properties: {
    maximumElasticWorkerCount: 3
    reserved: true
  }
  tags: {
    project: 'bi-automation'
    environment: environment
    managedBy: 'bicep'
  }
}

resource functionApp 'Microsoft.Web/sites@2023-01-01' = {
  name: functionAppName
  location: location
  kind: 'functionapp,linux'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: appServicePlan.id
    httpsOnly: true
    siteConfig: {
      pythonVersion: '3.10'
      linuxFxVersion: 'PYTHON|3.10'
      ftpsState: 'Disabled'
      minTlsVersion: '1.2'
      http20Enabled: true
      appSettings: [
        // Storage via managed identity (no shared keys)
        { name: 'AzureWebJobsStorage__accountName', value: storageAccountName }
        { name: 'WEBSITE_CONTENTAZUREFILECONNECTIONSTRING__accountName', value: storageAccountName }
        { name: 'WEBSITE_CONTENTSHARE', value: '${functionAppName}-content' }
        { name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }
        { name: 'FUNCTIONS_WORKER_RUNTIME', value: 'python' }
        { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: appInsightsConnectionString }
        // Non-secret config
        { name: 'KEY_VAULT_URI', value: keyVaultUri }
        { name: 'AZURE_OPENAI_ENDPOINT', value: openAiEndpoint }
        { name: 'SYNAPSE_SQL_ENDPOINT', value: synapseEndpoint }
        { name: 'SYNAPSE_SQL_USER', value: synapseSqlUser }
        { name: 'SYNAPSE_SQL_DATABASE', value: synapseDatabase }
        { name: 'ODBC_DRIVER', value: 'ODBC Driver 17 for SQL Server' }
        { name: 'ENVIRONMENT', value: environment }
        { name: 'ADF_NAME', value: adfName }
        { name: 'ADF_RESOURCE_GROUP', value: resourceGroup().name }
        { name: 'AZURE_SUBSCRIPTION_ID', value: subscription().subscriptionId }
        { name: 'STORAGE_ACCOUNT_NAME', value: storageAccountName }
        // Secrets via Key Vault references (requires RBAC on Key Vault)
        { name: 'SYNAPSE_SQL_PASSWORD', value: '@Microsoft.KeyVault(VaultName=${keyVaultName};SecretName=synapse-sql-password)' }
        { name: 'AI_API_KEY', value: '@Microsoft.KeyVault(VaultName=${keyVaultName};SecretName=ai-api-key)' }
        { name: 'ADO_PAT', value: '@Microsoft.KeyVault(VaultName=${keyVaultName};SecretName=ado-pat)' }
        { name: 'TEAMS_WEBHOOK_URL', value: '@Microsoft.KeyVault(VaultName=${keyVaultName};SecretName=teams-webhook-url)' }
        { name: 'AZURE_SEARCH_ENDPOINT', value: searchEndpoint }
      ]
      cors: {
        allowedOrigins: corsAllowedOrigins
        supportCredentials: false
      }
    }
  }
  tags: {
    project: 'bi-automation'
    environment: environment
    managedBy: 'bicep'
  }
}

output principalId string = functionApp.identity.principalId
output functionAppName string = functionApp.name
output defaultHostName string = functionApp.properties.defaultHostName
output appServicePlanId string = appServicePlan.id
