// Azure Functions (Premium Plan) - Test Automation Runtime
// Shares the same App Service Plan as the BI Function App

@description('Function App name')
param functionAppName string

@description('App Service Plan resource ID (shared with BI app)')
param appServicePlanId string

@description('Azure region')
param location string

@description('Storage account name')
param storageAccountName string

@description('Application Insights connection string')
param appInsightsConnectionString string

@description('Key Vault name')
param keyVaultName string

@description('Key Vault URI')
param keyVaultUri string

@description('Azure OpenAI endpoint')
param openAiEndpoint string

@description('Synapse SQL endpoint (for data tests)')
param synapseEndpoint string

@description('Synapse SQL admin user')
param synapseSqlUser string

@description('Synapse dedicated pool database name')
param synapseDatabase string

@description('Source SQL Server FQDN')
param sourceSqlServerFqdn string = ''

@description('Source SQL Database name')
param sourceSqlDbName string = ''

@description('Environment')
param environment string

resource functionApp 'Microsoft.Web/sites@2023-01-01' = {
  name: functionAppName
  location: location
  kind: 'functionapp,linux'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: appServicePlanId
    httpsOnly: true
    siteConfig: {
      pythonVersion: '3.10'
      linuxFxVersion: 'PYTHON|3.10'
      ftpsState: 'Disabled'
      minTlsVersion: '1.2'
      http20Enabled: true
      appSettings: [
        { name: 'AzureWebJobsStorage__accountName', value: storageAccountName }
        { name: 'WEBSITE_CONTENTAZUREFILECONNECTIONSTRING__accountName', value: storageAccountName }
        { name: 'WEBSITE_CONTENTSHARE', value: '${functionAppName}-content' }
        { name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }
        { name: 'FUNCTIONS_WORKER_RUNTIME', value: 'python' }
        { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: appInsightsConnectionString }
        { name: 'KEY_VAULT_URI', value: keyVaultUri }
        { name: 'AZURE_OPENAI_ENDPOINT', value: openAiEndpoint }
        { name: 'SYNAPSE_SQL_ENDPOINT', value: synapseEndpoint }
        { name: 'SYNAPSE_SQL_USER', value: synapseSqlUser }
        { name: 'SYNAPSE_SQL_DATABASE', value: synapseDatabase }
        { name: 'ODBC_DRIVER', value: 'ODBC Driver 17 for SQL Server' }
        { name: 'ENVIRONMENT', value: environment }
        { name: 'STORAGE_ACCOUNT_NAME', value: storageAccountName }
        // Secrets via Key Vault references
        { name: 'SYNAPSE_SQL_PASSWORD', value: '@Microsoft.KeyVault(VaultName=${keyVaultName};SecretName=synapse-sql-password)' }
        { name: 'AI_API_KEY', value: '@Microsoft.KeyVault(VaultName=${keyVaultName};SecretName=ai-api-key)' }
        { name: 'ADO_PAT', value: '@Microsoft.KeyVault(VaultName=${keyVaultName};SecretName=ado-pat)' }
        { name: 'TEAMS_WEBHOOK_URL', value: '@Microsoft.KeyVault(VaultName=${keyVaultName};SecretName=teams-webhook-url)' }
        // Test-specific settings
        { name: 'SOURCE_DB_SERVER', value: sourceSqlServerFqdn }
        { name: 'SOURCE_DB_NAME', value: sourceSqlDbName }
      ]
      cors: {
        allowedOrigins: ['https://portal.azure.com']
        supportCredentials: false
      }
    }
  }
  tags: {
    project: 'bi-automation'
    environment: environment
    managedBy: 'bicep'
    component: 'test-automation'
  }
}

output principalId string = functionApp.identity.principalId
output functionAppName string = functionApp.name
output defaultHostName string = functionApp.properties.defaultHostName
