"""Azure Data Factory client: deploys pipelines, datasets via ADF REST API."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Optional

import requests
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

logger = logging.getLogger(__name__)

ADF_API_VERSION = "2018-06-01"
TOKEN_TTL_SECONDS = 50 * 60  # Refresh tokens after 50 minutes (they last 60 min)


class ADFClient:
    """Deploys ADF pipelines and datasets using the ADF REST API."""

    def __init__(self):
        self._subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID", "")
        self._resource_group = os.environ.get("ADF_RESOURCE_GROUP", "")
        self._factory_name = os.environ.get("ADF_NAME", "")
        self._base_url = (
            f"https://management.azure.com/subscriptions/{self._subscription_id}"
            f"/resourceGroups/{self._resource_group}"
            f"/providers/Microsoft.DataFactory/factories/{self._factory_name}"
        )
        self._token = None
        self._token_acquired_at: float = 0.0

    @property
    def is_configured(self) -> bool:
        return bool(self._subscription_id and self._resource_group and self._factory_name)

    def _get_token(self) -> str:
        if self._token and (time.time() - self._token_acquired_at) < TOKEN_TTL_SECONDS:
            return self._token
        try:
            cred = ManagedIdentityCredential()
            token = cred.get_token("https://management.azure.com/.default")
            self._token = token.token
        except Exception as e:
            logger.debug("ManagedIdentityCredential failed, falling back to DefaultAzureCredential: %s", e)
            cred = DefaultAzureCredential()
            token = cred.get_token("https://management.azure.com/.default")
            self._token = token.token
        self._token_acquired_at = time.time()
        return self._token

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

    def deploy_pipeline(self, pipeline_name: str, pipeline_json: dict) -> dict:
        """Deploy a pipeline to ADF. Creates or updates."""
        if not self.is_configured:
            logger.warning("ADF not configured, skipping pipeline deployment")
            return {"status": "skipped", "reason": "ADF not configured"}

        url = f"{self._base_url}/pipelines/{pipeline_name}?api-version={ADF_API_VERSION}"

        # ADF expects the pipeline definition under "properties"
        body = pipeline_json
        if "properties" not in body:
            body = {"properties": pipeline_json}

        try:
            resp = requests.put(url, headers=self._headers(), json=body, timeout=30)
            if resp.status_code in (200, 201, 202):
                logger.info("ADF pipeline deployed: %s", pipeline_name)
                return {"status": "deployed", "pipeline_name": pipeline_name}
            else:
                error_msg = resp.text[:500]
                logger.error("ADF pipeline deploy failed: %s %s", resp.status_code, error_msg)
                return {"status": "failed", "error": error_msg}
        except Exception as e:
            logger.error("ADF pipeline deploy error: %s", e)
            return {"status": "failed", "error": str(e)}

    def deploy_dataset(self, dataset_name: str, dataset_json: dict) -> dict:
        """Deploy a dataset to ADF."""
        if not self.is_configured:
            return {"status": "skipped", "reason": "ADF not configured"}

        url = f"{self._base_url}/datasets/{dataset_name}?api-version={ADF_API_VERSION}"
        body = dataset_json
        if "properties" not in body:
            body = {"properties": dataset_json}

        try:
            resp = requests.put(url, headers=self._headers(), json=body, timeout=30)
            if resp.status_code in (200, 201, 202):
                logger.info("ADF dataset deployed: %s", dataset_name)
                return {"status": "deployed", "dataset_name": dataset_name}
            else:
                return {"status": "failed", "error": resp.text[:500]}
        except Exception as e:
            return {"status": "failed", "error": str(e)}

    def deploy_bronze_pipeline(self, story_id: str, pipeline_json_str: str) -> dict:
        """Deploy a complete Bronze pipeline with datasets from generated JSON."""
        try:
            pipeline_def = json.loads(pipeline_json_str)
        except json.JSONDecodeError as e:
            return {"status": "failed", "error": f"Invalid JSON: {e}"}

        pipeline_name = pipeline_def.get("name", f"pl_bronze_{story_id}")
        properties = pipeline_def.get("properties", pipeline_def)

        # Deploy source datasets (SQL tables)
        datasets_deployed = []
        for activity in properties.get("activities", []):
            for inp in activity.get("inputs", []):
                ds_name = inp.get("referenceName", "")
                if ds_name and ds_name.startswith("SqlMI_"):
                    parts = ds_name.replace("SqlMI_", "").split("_", 1)
                    schema = parts[0] if len(parts) > 0 else "dbo"
                    table = parts[1] if len(parts) > 1 else ""
                    ds_def = {
                        "properties": {
                            "type": "AzureSqlTable",
                            "linkedServiceName": {
                                "referenceName": "ls_source_sqldb",
                                "type": "LinkedServiceReference",
                            },
                            "typeProperties": {
                                "schema": schema,
                                "table": table,
                            },
                        },
                    }
                    result = self.deploy_dataset(ds_name, ds_def)
                    datasets_deployed.append(result)

            # Deploy sink datasets (Parquet on ADLS)
            for out in activity.get("outputs", []):
                ds_name = out.get("referenceName", "")
                if ds_name and ds_name.startswith("ADLS_Parquet_"):
                    table_name = ds_name.replace("ADLS_Parquet_", "")
                    ds_def = {
                        "properties": {
                            "type": "Parquet",
                            "linkedServiceName": {
                                "referenceName": "ls_adls_bronze",
                                "type": "LinkedServiceReference",
                            },
                            "typeProperties": {
                                "location": {
                                    "type": "AzureBlobFSLocation",
                                    "folderPath": f"bronze/{table_name}",
                                    "fileSystem": "bronze",
                                },
                                "compressionCodec": "snappy",
                            },
                            "schema": [],
                        },
                    }
                    result = self.deploy_dataset(ds_name, ds_def)
                    datasets_deployed.append(result)

        # Deploy the pipeline itself
        pipeline_body = {"name": pipeline_name, "properties": properties}
        pipeline_result = self.deploy_pipeline(pipeline_name, pipeline_body)

        # Deploy a daily schedule trigger
        trigger_result = self.deploy_schedule_trigger(
            pipeline_name=pipeline_name,
            story_id=story_id,
        )

        return {
            "pipeline": pipeline_result,
            "datasets": datasets_deployed,
            "trigger": trigger_result,
            "pipeline_name": pipeline_name,
        }

    def deploy_schedule_trigger(
        self,
        pipeline_name: str,
        story_id: str,
        schedule_hour: int = 2,
        schedule_minute: int = 0,
        timezone: str = "UTC",
    ) -> dict:
        """Deploy a daily schedule trigger for the pipeline."""
        if not self.is_configured:
            return {"status": "skipped", "reason": "ADF not configured"}

        trigger_name = f"tr_daily_{pipeline_name}"
        trigger_body = {
            "properties": {
                "type": "ScheduleTrigger",
                "typeProperties": {
                    "recurrence": {
                        "frequency": "Day",
                        "interval": 1,
                        "startTime": "2026-04-03T00:00:00Z",
                        "timeZone": timezone,
                        "schedule": {
                            "hours": [schedule_hour],
                            "minutes": [schedule_minute],
                        },
                    },
                },
                "pipelines": [
                    {
                        "pipelineReference": {
                            "referenceName": pipeline_name,
                            "type": "PipelineReference",
                        },
                        "parameters": {},
                    }
                ],
                "annotations": [story_id, "auto-generated"],
            },
        }

        url = f"{self._base_url}/triggers/{trigger_name}?api-version={ADF_API_VERSION}"
        try:
            resp = requests.put(url, headers=self._headers(), json=trigger_body, timeout=30)
            if resp.status_code in (200, 201, 202):
                logger.info("ADF trigger deployed: %s (daily at %02d:%02d %s)",
                            trigger_name, schedule_hour, schedule_minute, timezone)
                return {"status": "deployed", "trigger_name": trigger_name}
            else:
                logger.error("ADF trigger deploy failed: %s %s", resp.status_code, resp.text[:500])
                return {"status": "failed", "error": resp.text[:500]}
        except Exception as e:
            logger.error("ADF trigger deploy error: %s", e)
            return {"status": "failed", "error": str(e)}
