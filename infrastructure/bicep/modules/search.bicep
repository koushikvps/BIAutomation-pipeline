// ============================================================
// MODULE: Azure AI Search (RAG vector store)
// ============================================================

@description('Azure AI Search service name')
param searchServiceName string

@description('Azure region')
param location string

@description('Environment tag')
param environment string

@description('SKU for Azure AI Search')
@allowed(['free', 'basic', 'standard', 'standard2', 'standard3'])
param sku string = 'basic'

resource searchService 'Microsoft.Search/searchServices@2024-06-01-preview' = {
  name: searchServiceName
  location: location
  sku: {
    name: sku
  }
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    replicaCount: 1
    partitionCount: 1
    hostingMode: 'default'
    publicNetworkAccess: 'enabled'
    semanticSearch: sku == 'free' ? 'disabled' : 'free'
    disableLocalAuth: true
  }
  tags: {
    environment: environment
    component: 'rag-knowledge-base'
  }
}

output searchServiceId string = searchService.id
output searchEndpoint string = 'https://${searchService.name}.search.windows.net'
output searchPrincipalId string = searchService.identity.principalId
