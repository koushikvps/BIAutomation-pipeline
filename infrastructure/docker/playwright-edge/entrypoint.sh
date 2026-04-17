#!/bin/bash
set -e

echo "=== Playwright Test Runner ==="
echo "Run ID: $RUN_ID"
echo "App URL: $APP_URL"

# Download test package from blob storage
echo "Downloading tests..."
az storage blob download \
    --account-name "$STORAGE_ACCOUNT" \
    --account-key "$STORAGE_KEY" \
    --container-name test-artifacts \
    --name "$TEST_BLOB_PATH" \
    --file /workspace/tests.zip \
    --no-progress

# Extract
mkdir -p /workspace/tests
unzip -o /workspace/tests.zip -d /workspace/tests/
cd /workspace/tests

# Install any extra requirements
if [ -f requirements.txt ]; then
    pip install -r requirements.txt --quiet
fi

# Create screenshots dir
mkdir -p screenshots

# Run tests
echo "Running Playwright tests with Edge..."
python -m pytest \
    --tb=short \
    --junitxml=results.xml \
    --browser-channel msedge \
    -v \
    2>&1 | tee output.log || true

echo "Tests completed. Uploading results..."

# Upload results
az storage blob upload \
    --account-name "$STORAGE_ACCOUNT" \
    --account-key "$STORAGE_KEY" \
    --container-name test-artifacts \
    --name "test-runs/$RUN_ID/results.xml" \
    --file results.xml \
    --overwrite \
    --no-progress 2>/dev/null || echo "No results.xml"

az storage blob upload \
    --account-name "$STORAGE_ACCOUNT" \
    --account-key "$STORAGE_KEY" \
    --container-name test-artifacts \
    --name "test-runs/$RUN_ID/output.log" \
    --file output.log \
    --overwrite \
    --no-progress 2>/dev/null || echo "No output.log"

# Upload screenshots
if [ -d screenshots ] && [ "$(ls -A screenshots/)" ]; then
    az storage blob upload-batch \
        --account-name "$STORAGE_ACCOUNT" \
        --account-key "$STORAGE_KEY" \
        --destination test-artifacts \
        --destination-path "test-runs/$RUN_ID/screenshots" \
        --source screenshots/ \
        --overwrite \
        --no-progress 2>/dev/null || echo "No screenshots"
fi

echo "=== DONE ==="
