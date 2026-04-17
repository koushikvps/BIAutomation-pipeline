// ADLS Gen2 Storage Account for Bronze layer + Function App storage

@description('Storage account name')
param storageAccountName string

@description('Azure region')
param location string

@description('Environment tag')
param environment string

// Phase 2: Add private endpoints for fully private access (requires VNet)
@description('Enable private endpoints (Phase 2 - requires VNet integration)')
param enablePrivateEndpoints bool = false

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_GRS'
  }
  properties: {
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    accessTier: 'Hot'
    networkAcls: {
      defaultAction: 'Deny'
      bypass: 'AzureServices'
    }
  }
  tags: {
    project: 'bi-automation'
    environment: environment
    managedBy: 'bicep'
  }
}

// Bronze container
resource bronzeContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name: '${storageAccount.name}/default/bronze'
  properties: {
    publicAccess: 'None'
  }
}

// Silver staging container (if needed for intermediate files)
resource silverContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name: '${storageAccount.name}/default/silver-staging'
  properties: {
    publicAccess: 'None'
  }
}

// Agent artifacts container (build plans, reports)
resource artifactsContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name: '${storageAccount.name}/default/agent-artifacts'
  properties: {
    publicAccess: 'None'
  }
}

output storageAccountId string = storageAccount.id
output storageAccountName string = storageAccount.name
output dfsEndpoint string = storageAccount.properties.primaryEndpoints.dfs
output blobEndpoint string = storageAccount.properties.primaryEndpoints.blob
