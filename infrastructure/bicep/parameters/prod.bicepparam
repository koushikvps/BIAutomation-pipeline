using '../main.bicep'

// Production parameters - update during lift-and-shift
param environment = 'prod'
param prefix = 'biautomation'
param location = 'eastus'
param synapseSqlAdminObjectId = '<PROD_AZURE_AD_OBJECT_ID>'
param sqlAdminUsername = '<PROD_SQL_ADMIN>'
param sqlAdminPassword = '<PROD_SQL_PASSWORD>'
param openAiModelDeploymentName = 'gpt-4'
