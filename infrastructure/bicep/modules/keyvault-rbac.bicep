// Key Vault RBAC assignments (separate to break circular dependency)

@description('Key Vault name')
param keyVaultName string

@description('BI Function App Managed Identity principal ID')
param functionAppPrincipalId string

@description('Test Function App Managed Identity principal ID')
param testFunctionAppPrincipalId string = ''

@description('Synapse Managed Identity principal ID')
param synapsePrincipalId string

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' existing = {
  name: keyVaultName
}

// RBAC: Function App can read secrets
resource functionAppSecretReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, functionAppPrincipalId, 'secret-reader')
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6') // Key Vault Secrets User
    principalId: functionAppPrincipalId
    principalType: 'ServicePrincipal'
  }
}

// RBAC: Test Function App can read secrets
resource testFunctionAppSecretReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(testFunctionAppPrincipalId)) {
  name: guid(keyVault.id, testFunctionAppPrincipalId, 'secret-reader')
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalId: testFunctionAppPrincipalId
    principalType: 'ServicePrincipal'
  }
}

// RBAC: Synapse can read secrets
resource synapseSecretReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, synapsePrincipalId, 'secret-reader')
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalId: synapsePrincipalId
    principalType: 'ServicePrincipal'
  }
}
