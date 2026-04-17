using '../main.bicep'

param environment = 'dev'
param prefix = 'biautomation'
param location = 'westus2'
param synapseSqlAdminObjectId = '<YOUR_AZURE_AD_OBJECT_ID>'
param sqlAdminUsername = 'sqladmin'
// SECURITY: Never commit real passwords. Supply via CI/CD pipeline variable or --parameters override.
param sqlAdminPassword = '<REPLACE_WITH_SECURE_PASSWORD>'
param openAiModelDeploymentName = 'gpt-4o'
