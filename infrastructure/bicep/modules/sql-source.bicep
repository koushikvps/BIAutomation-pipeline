// Azure SQL Database (Source System)
// Basic tier for development. Upgrade to Standard/Premium for production.

@description('SQL Server name')
param sqlServerName string

@description('Database name')
param sqlDbName string

@description('Azure region')
param location string

@description('SQL admin username')
@secure()
param adminUsername string

@description('SQL admin password')
@secure()
param adminPassword string

@description('Azure AD admin object ID for SQL Server')
param aadAdminObjectId string = ''

@description('Azure AD admin display name')
param aadAdminDisplayName string = 'SQL AAD Admin'

@description('Azure AD admin login (UPN or group name)')
param aadAdminLogin string = ''

@description('Environment tag')
param environment string = 'dev'

@description('Log Analytics workspace ID for audit logs')
param logAnalyticsWorkspaceId string = ''

// Phase 2: Add private endpoints for fully private access (requires VNet)
@description('Enable private endpoints (Phase 2 - requires VNet integration)')
param enablePrivateEndpoints bool = false

resource sqlServer 'Microsoft.Sql/servers@2023-05-01-preview' = {
  name: sqlServerName
  location: location
  properties: {
    administratorLogin: adminUsername
    administratorLoginPassword: adminPassword
    minimalTlsVersion: '1.2'
  }
  tags: {
    project: 'bi-automation'
    environment: environment
    managedBy: 'bicep'
    role: 'simulated-source'
  }
}

// Azure AD administrator for SQL Server
resource sqlAadAdmin 'Microsoft.Sql/servers/administrators@2023-05-01-preview' = if (!empty(aadAdminObjectId)) {
  name: 'ActiveDirectory'
  parent: sqlServer
  properties: {
    administratorType: 'ActiveDirectory'
    login: aadAdminLogin
    sid: aadAdminObjectId
    tenantId: subscription().tenantId
  }
}

// Allow Azure services
resource firewallAzure 'Microsoft.Sql/servers/firewallRules@2023-05-01-preview' = {
  name: 'AllowAzureServices'
  parent: sqlServer
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

// Source database (Basic tier)
resource sqlDatabase 'Microsoft.Sql/servers/databases@2023-05-01-preview' = {
  name: sqlDbName
  parent: sqlServer
  location: location
  sku: {
    name: 'Basic'
    tier: 'Basic'
    capacity: 5
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    maxSizeBytes: 2147483648 // 2GB
  }
  tags: {
    project: 'bi-automation'
    environment: environment
    managedBy: 'bicep'
    role: 'simulated-source'
  }
}

// Auditing policy for SQL Server
resource sqlAuditingSettings 'Microsoft.Sql/servers/auditingSettings@2023-05-01-preview' = {
  name: 'default'
  parent: sqlServer
  properties: {
    state: 'Enabled'
    isAzureMonitorTargetEnabled: true
    retentionDays: 90
  }
}

// Advanced Threat Protection
resource sqlThreatDetection 'Microsoft.Sql/servers/advancedThreatProtectionSettings@2023-05-01-preview' = {
  name: 'Default'
  parent: sqlServer
  properties: {
    state: 'Enabled'
  }
}

// SQL Server diagnostic settings (if Log Analytics workspace is provided)
resource sqlDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (!empty(logAnalyticsWorkspaceId)) {
  name: '${sqlServerName}-diag'
  scope: sqlDatabase
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      {
        category: 'SQLSecurityAuditEvents'
        enabled: true
      }
    ]
    metrics: [
      {
        category: 'Basic'
        enabled: true
      }
    ]
  }
}

output serverFqdn string = sqlServer.properties.fullyQualifiedDomainName
output databaseName string = sqlDatabase.name
output serverName string = sqlServer.name
