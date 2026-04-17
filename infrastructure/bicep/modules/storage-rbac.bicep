// Storage Account RBAC assignments
// Required because storage has allowSharedKeyAccess: false — all access via Managed Identity

@description('Storage account name')
param storageAccountName string

@description('ADF Managed Identity principal ID — needs Storage Blob Data Contributor')
param adfPrincipalId string

@description('BI Function App principal ID — needs Storage Blob Data Owner for AzureWebJobsStorage')
param functionAppPrincipalId string

@description('Test Function App principal ID')
param testFunctionAppPrincipalId string = ''

@description('Synapse principal ID — needs Storage Blob Data Contributor for external tables')
param synapsePrincipalId string

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: storageAccountName
}

// Storage Blob Data Contributor: ba92f5b4-2d11-453d-a403-e96b0029c9fe
// Storage Blob Data Owner: b7e6dc6d-f1e8-4753-8033-0f276bb0955b
var blobDataContributor = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
var blobDataOwner = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'b7e6dc6d-f1e8-4753-8033-0f276bb0955b')

// ADF needs Blob Data Contributor to write Parquet to bronze container
resource adfStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, adfPrincipalId, 'blob-contributor')
  scope: storageAccount
  properties: {
    roleDefinitionId: blobDataContributor
    principalId: adfPrincipalId
    principalType: 'ServicePrincipal'
  }
}

// BI Function App needs Blob Data Owner for AzureWebJobsStorage (Durable Tasks, triggers)
resource funcStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, functionAppPrincipalId, 'blob-owner')
  scope: storageAccount
  properties: {
    roleDefinitionId: blobDataOwner
    principalId: functionAppPrincipalId
    principalType: 'ServicePrincipal'
  }
}

// Test Function App needs Blob Data Owner for AzureWebJobsStorage
resource testFuncStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(testFunctionAppPrincipalId)) {
  name: guid(storageAccount.id, testFunctionAppPrincipalId, 'blob-owner')
  scope: storageAccount
  properties: {
    roleDefinitionId: blobDataOwner
    principalId: testFunctionAppPrincipalId
    principalType: 'ServicePrincipal'
  }
}

// Synapse needs Blob Data Contributor for external tables reading from ADLS
resource synapseStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, synapsePrincipalId, 'blob-contributor')
  scope: storageAccount
  properties: {
    roleDefinitionId: blobDataContributor
    principalId: synapsePrincipalId
    principalType: 'ServicePrincipal'
  }
}
