// Azure Key Vault for secrets management

@description('Key Vault name')
param keyVaultName string

@description('Azure region')
param location string

@description('Allow public network access (false for production with private endpoints)')
param allowPublicAccess bool = false

@description('Log Analytics workspace ID for diagnostic logs')
param logAnalyticsWorkspaceId string

@description('Environment tag')
param environment string = 'dev'

// Phase 2: Add private endpoints for fully private access (requires VNet)
@description('Enable private endpoints (Phase 2 - requires VNet integration)')
param enablePrivateEndpoints bool = false

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: subscription().tenantId
    enableRbacAuthorization: true
    enableSoftDelete: true
    enablePurgeProtection: true
    softDeleteRetentionInDays: 90
    networkAcls: {
      defaultAction: allowPublicAccess ? 'Allow' : 'Deny'
      bypass: 'AzureServices'
    }
  }
  tags: {
    project: 'bi-automation'
    environment: environment
    managedBy: 'bicep'
  }
}

// Diagnostic settings for audit logging — sends to Log Analytics workspace
resource diagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: '${keyVaultName}-diag'
  scope: keyVault
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
      }
    ]
    logs: [
      {
        categoryGroup: 'audit'
        enabled: true
      }
    ]
  }
}

output keyVaultUri string = keyVault.properties.vaultUri
output keyVaultName string = keyVault.name
