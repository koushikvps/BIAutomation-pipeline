#!/bin/bash
# ============================================================
# Deploy Synapse BI Automation Framework to Azure
# Usage: ./deploy.sh <environment> [resource-group-name]
# Example: ./deploy.sh dev <resource-group-name>
# ============================================================

set -euo pipefail

ENV="${1:-dev}"
RG_NAME="${2:-biautomation-${ENV}-rg}"
LOCATION="eastus"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BICEP_DIR="${SCRIPT_DIR}/../bicep"

echo "============================================"
echo "Deploying BI Automation Framework"
echo "Environment: ${ENV}"
echo "Resource Group: ${RG_NAME}"
echo "Location: ${LOCATION}"
echo "============================================"

# Step 1: Ensure logged in
echo "[1/5] Checking Azure CLI login..."
az account show > /dev/null 2>&1 || { echo "ERROR: Not logged in. Run 'az login' first."; exit 1; }
echo "Logged in as: $(az account show --query user.name -o tsv)"
echo "Subscription: $(az account show --query name -o tsv)"

# Step 2: Create resource group
echo "[2/5] Creating resource group..."
az group create --name "${RG_NAME}" --location "${LOCATION}" --tags project=bi-automation environment="${ENV}"

# Step 3: Get current user's Azure AD object ID (for Synapse admin)
echo "[3/5] Getting Azure AD object ID..."
OBJECT_ID=$(az ad signed-in-user show --query id -o tsv)
echo "Azure AD Object ID: ${OBJECT_ID}"

# Step 4: Deploy Bicep
echo "[4/5] Deploying infrastructure (this takes 5-10 minutes)..."
az deployment group create \
  --resource-group "${RG_NAME}" \
  --template-file "${BICEP_DIR}/main.bicep" \
  --parameters "${BICEP_DIR}/parameters/${ENV}.bicepparam" \
  --parameters synapseSqlAdminObjectId="${OBJECT_ID}" \
  --name "bi-automation-${ENV}-$(date +%Y%m%d%H%M%S)" \
  --verbose

# Step 5: Output results
echo "[5/5] Deployment complete. Outputs:"
az deployment group show \
  --resource-group "${RG_NAME}" \
  --name "$(az deployment group list --resource-group ${RG_NAME} --query '[0].name' -o tsv)" \
  --query properties.outputs

echo ""
echo "============================================"
echo "NEXT STEPS:"
echo "1. Update dev.bicepparam with your password"
echo "2. Run: cd ../.. && ./infrastructure/scripts/seed-source-data.sh ${ENV}"
echo "3. Run: cd ../.. && ./infrastructure/scripts/deploy-catalog.sh ${ENV}"
echo "============================================"
