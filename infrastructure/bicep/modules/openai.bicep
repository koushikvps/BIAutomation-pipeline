// Azure OpenAI Service (Agent brain)

@description('Azure OpenAI resource name')
param openAiName string

@description('Azure region')
param location string

@description('Model deployment name')
param modelDeploymentName string

@description('Environment tag')
param environment string = 'dev'

// Phase 2: Add private endpoints for fully private access (requires VNet)
@description('Enable private endpoints (Phase 2 - requires VNet integration)')
param enablePrivateEndpoints bool = false

resource openAi 'Microsoft.CognitiveServices/accounts@2023-10-01-preview' = {
  name: openAiName
  location: location
  kind: 'OpenAI'
  sku: {
    name: 'S0'
  }
  properties: {
    customSubDomainName: openAiName
    publicNetworkAccess: 'Enabled'
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

// GPT-4 deployment
resource gpt4Deployment 'Microsoft.CognitiveServices/accounts/deployments@2023-10-01-preview' = {
  name: modelDeploymentName
  parent: openAi
  sku: {
    name: 'Standard'
    capacity: 10 // 10K TPM
  }
  properties: {
    model: {
      format: 'OpenAI'
      name: 'gpt-4o'
      version: '2024-08-06'
    }
  }
}

output endpoint string = openAi.properties.endpoint
output resourceName string = openAi.name
