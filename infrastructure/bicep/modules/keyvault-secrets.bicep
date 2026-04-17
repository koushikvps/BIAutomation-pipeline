// Key Vault secrets population module
// Stores all sensitive values as Key Vault secrets for reference by other resources

@description('Key Vault name')
param keyVaultName string

@secure()
@description('SQL admin password')
param sqlAdminPassword string

@secure()
@description('AI API key (optional, empty if using managed identity)')
param aiApiKey string = ''

@secure()
@description('ADO PAT (optional)')
param adoPat string = ''

@secure()
@description('Teams webhook URL (optional)')
param teamsWebhookUrl string = ''

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' existing = {
  name: keyVaultName
}

resource secretSqlPassword 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: keyVault
  name: 'synapse-sql-password'
  properties: {
    value: sqlAdminPassword
    attributes: {
      enabled: true
    }
  }
}

resource secretSqlSourcePassword 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: keyVault
  name: 'source-sql-password'
  properties: {
    value: sqlAdminPassword
    attributes: {
      enabled: true
    }
  }
}

resource secretAiApiKey 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = if (!empty(aiApiKey)) {
  parent: keyVault
  name: 'ai-api-key'
  properties: {
    value: aiApiKey
    attributes: {
      enabled: true
    }
  }
}

resource secretAdoPat 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = if (!empty(adoPat)) {
  parent: keyVault
  name: 'ado-pat'
  properties: {
    value: adoPat
    attributes: {
      enabled: true
    }
  }
}

resource secretTeamsWebhook 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = if (!empty(teamsWebhookUrl)) {
  parent: keyVault
  name: 'teams-webhook-url'
  properties: {
    value: teamsWebhookUrl
    attributes: {
      enabled: true
    }
  }
}

output sqlPasswordSecretUri string = secretSqlPassword.properties.secretUri
output sourcePasswordSecretUri string = secretSqlSourcePassword.properties.secretUri
