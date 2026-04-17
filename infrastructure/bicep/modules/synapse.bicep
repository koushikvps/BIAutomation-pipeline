// Azure Synapse Analytics Workspace with Dedicated SQL Pool

@description('Synapse workspace name')
param workspaceName string

@description('Azure region')
param location string

@description('ADLS Gen2 storage account resource ID')
param storageAccountId string

@description('ADLS Gen2 DFS endpoint URL')
param storageAccountUrl string

@description('Azure AD object ID for SQL admin')
param sqlAdminObjectId string

@secure()
@description('SQL admin password')
param sqlAdminPassword string

@description('Dedicated SQL pool performance level')
param sqlPoolSku string = 'DW100c'

@description('SQL administrator login name')
param sqlAdministratorLogin string = 'sqladmin'

@description('Dedicated SQL pool name')
param sqlPoolName string = 'bipool'

@description('Enable Azure AD-only authentication (recommended for production)')
param azureADOnlyAuthentication bool = true

@description('Environment tag')
param environment string = 'dev'

// Phase 2: Add private endpoints for fully private access (requires VNet)
@description('Enable private endpoints (Phase 2 - requires VNet integration)')
param enablePrivateEndpoints bool = false

resource synapseWorkspace 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name: workspaceName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    defaultDataLakeStorage: {
      accountUrl: storageAccountUrl
      filesystem: 'bronze'
      resourceId: storageAccountId
    }
    azureADOnlyAuthentication: azureADOnlyAuthentication
    sqlAdministratorLogin: sqlAdministratorLogin
    sqlAdministratorLoginPassword: sqlAdminPassword
    managedResourceGroupName: '${workspaceName}-managed'
  }
  tags: {
    project: 'bi-automation'
    environment: environment
    managedBy: 'bicep'
  }
}

// Dedicated SQL Pool (DW100c - smallest, ~$1.20/hr, pausable)
resource sqlPool 'Microsoft.Synapse/workspaces/sqlPools@2021-06-01' = {
  name: sqlPoolName
  parent: synapseWorkspace
  location: location
  sku: {
    name: sqlPoolSku
    capacity: 0
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    createMode: 'Default'
  }
  tags: {
    project: 'bi-automation'
    environment: environment
    managedBy: 'bicep'
  }
}

// Azure AD admin
resource synapseAadAdmin 'Microsoft.Synapse/workspaces/administrators@2021-06-01' = {
  name: 'activeDirectory'
  parent: synapseWorkspace
  properties: {
    sid: sqlAdminObjectId
    tenantId: subscription().tenantId
  }
}

// Firewall: Allow Azure services only
resource firewallAllowAzure 'Microsoft.Synapse/workspaces/firewallRules@2021-06-01' = {
  name: 'AllowAllWindowsAzureIps'
  parent: synapseWorkspace
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

// For client IP access, add specific firewall rules:
// resource firewallClientIp 'Microsoft.Synapse/workspaces/firewallRules@2021-06-01' = {
//   name: 'ClientOfficeIP'
//   parent: synapseWorkspace
//   properties: { startIpAddress: 'x.x.x.x', endIpAddress: 'x.x.x.x' }
// }

output principalId string = synapseWorkspace.identity.principalId
output sqlEndpoint string = synapseWorkspace.properties.connectivityEndpoints.sql
output sqlPoolEndpoint string = '${workspaceName}.sql.azuresynapse.net'
output sqlPoolName string = sqlPool.name
output sqlAdministratorLogin string = sqlAdministratorLogin
output sqlPoolDatabaseName string = sqlPoolName
output devEndpoint string = synapseWorkspace.properties.connectivityEndpoints.dev
output workspaceName string = synapseWorkspace.name
