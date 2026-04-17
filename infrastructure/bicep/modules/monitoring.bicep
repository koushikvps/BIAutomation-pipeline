// Log Analytics + Application Insights for agent monitoring

@description('Log Analytics workspace name')
param logAnalyticsName string

@description('Application Insights name')
param appInsightsName string

@description('Azure region')
param location string

@description('Environment tag')
param environment string = 'dev'

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: logAnalyticsName
  location: location
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 90
  }
  tags: {
    project: 'bi-automation'
    environment: environment
    managedBy: 'bicep'
  }
}

resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: appInsightsName
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: logAnalytics.id
    RetentionInDays: 30
  }
  tags: {
    project: 'bi-automation'
    environment: environment
    managedBy: 'bicep'
  }
}

output logAnalyticsId string = logAnalytics.id
// Use connectionString instead of deprecated InstrumentationKey
output appInsightsConnectionString string = appInsights.properties.ConnectionString
