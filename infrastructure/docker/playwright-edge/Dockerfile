FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

RUN apt-get update && apt-get install -y \
    unzip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Azure CLI (for blob upload/download)
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# Install Edge browser for Playwright
RUN playwright install msedge

WORKDIR /workspace

# Pre-install common test dependencies
RUN pip install pytest pytest-playwright pytest-asyncio openpyxl requests

# Entry point: download tests, run, upload results
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
