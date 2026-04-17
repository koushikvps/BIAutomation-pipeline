// Azure Data Factory - Pipeline Orchestration

@description('Data Factory name')
param dataFactoryName string

@description('Azure region')
param location string

@description('Source SQL Server FQDN')
param sourceSqlServerFqdn string

@description('Source database name')
param sourceSqlDbName string

@description('SQL admin username')
param sqlAdminUsername string

@description('Storage account name (ADLS Gen2)')
param storageAccountName string

@description('Key Vault name for secret references')
param keyVaultName string

@description('Key Vault base URI')
param keyVaultUri string

@description('Environment tag')
param environment string = 'dev'

// Phase 2: Add private endpoints for fully private access (requires VNet)
@description('Enable private endpoints (Phase 2 - requires VNet integration)')
param enablePrivateEndpoints bool = false

resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {}
  tags: {
    project: 'bi-automation'
    environment: environment
    managedBy: 'bicep'
  }
}

// Linked Service: Source SQL Database (password via Key Vault reference)
resource lsSourceSql 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: dataFactory
  name: 'ls_source_sqldb'
  properties: {
    type: 'AzureSqlDatabase'
    typeProperties: {
      connectionString: 'Server=tcp:${sourceSqlServerFqdn},1433;Database=${sourceSqlDbName};User ID=${sqlAdminUsername};Encrypt=true;TrustServerCertificate=false;Connection Timeout=30;'
      password: {
        type: 'AzureKeyVaultSecret'
        store: {
          referenceName: 'ls_keyvault'
          type: 'LinkedServiceReference'
        }
        secretName: 'source-sql-password'
      }
    }
  }
  dependsOn: [lsKeyVault]
}

// Linked Service: Azure Key Vault (for secret references)
resource lsKeyVault 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: dataFactory
  name: 'ls_keyvault'
  properties: {
    type: 'AzureKeyVault'
    typeProperties: {
      baseUrl: keyVaultUri
    }
  }
}

// Linked Service: ADLS Gen2 (Bronze landing zone) - uses Managed Identity
resource lsAdls 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: dataFactory
  name: 'ls_adls_bronze'
  properties: {
    type: 'AzureBlobFS'
    typeProperties: {
      url: 'https://${storageAccountName}.dfs.core.windows.net'
    }
  }
}

output dataFactoryName string = dataFactory.name
output dataFactoryId string = dataFactory.id
output dataFactoryPrincipalId string = dataFactory.identity.principalId
