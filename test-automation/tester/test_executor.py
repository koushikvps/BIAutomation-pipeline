"""Test Executor — Runs Playwright tests via Azure Container Instances.

Packages generated test scripts, pushes to blob storage, triggers an ACI
container with Playwright + Edge, and collects results.
"""

from __future__ import annotations

import io
import json
import logging
import os
import time
import zipfile

import requests
from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)

ACI_API_VERSION = "2023-05-01"


class TestExecutor:
    """Manages Playwright test execution on Azure Container Instances."""

    def __init__(self):
        self._subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID", "")
        self._resource_group = os.environ.get("RESOURCE_GROUP", "")
        self._storage_account = os.environ.get("STORAGE_ACCOUNT_NAME", "")
        self._storage_key = os.environ.get("STORAGE_ACCOUNT_KEY", "")
        self._acr_server = os.environ.get("ACR_SERVER", "")
        self._acr_username = os.environ.get("ACR_USERNAME", "")
        self._acr_password = os.environ.get("ACR_PASSWORD", "")
        self._container_image = os.environ.get("PLAYWRIGHT_IMAGE", "biautomationdevacr.azurecr.io/playwright-edge:latest")
        self._edge_profile_path = os.environ.get("EDGE_PROFILE_BLOB", "")

    def package_tests(self, generated_code: dict, run_id: str) -> str:
        """Package generated test files into a zip and upload to blob storage."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            # conftest.py
            if generated_code.get("conftest"):
                zf.writestr("conftest.py", generated_code["conftest"])

            # Test files
            for tf in generated_code.get("test_files", []):
                zf.writestr(tf["filename"], tf["content"])

            # Page objects
            for po in generated_code.get("page_objects", []):
                zf.writestr(po["filename"], po["content"])

            # pytest.ini
            zf.writestr("pytest.ini", "[pytest]\nasyncio_mode = auto\n")

            # requirements.txt for the container
            zf.writestr("requirements.txt", "pytest\npytest-playwright\npytest-asyncio\nopenpyxl\n")

        buf.seek(0)
        blob_path = f"test-runs/{run_id}/tests.zip"
        self._upload_blob("test-artifacts", blob_path, buf.read())
        logger.info("Packaged tests to blob: %s", blob_path)
        return blob_path

    def run_tests(self, run_id: str, app_url: str, blob_path: str) -> dict:
        """Trigger ACI container to execute tests and wait for results."""
        container_name = f"test-{run_id[:8]}"

        # Create ACI container group
        aci_body = {
            "location": os.environ.get("AZURE_REGION", "westus2"),
            "properties": {
                "containers": [
                    {
                        "name": "playwright",
                        "properties": {
                            "image": self._container_image,
                            "resources": {
                                "requests": {"cpu": 2, "memoryInGB": 4},
                            },
                            "environmentVariables": [
                                {"name": "RUN_ID", "value": run_id},
                                {"name": "APP_URL", "value": app_url},
                                {"name": "STORAGE_ACCOUNT", "value": self._storage_account},
                                {"name": "STORAGE_KEY", "secureValue": self._storage_key},
                                {"name": "TEST_BLOB_PATH", "value": blob_path},
                                {"name": "RESULTS_CONTAINER", "value": "test-artifacts"},
                            ],
                            "command": [
                                "/bin/bash", "-c",
                                "cd /workspace && "
                                "az storage blob download --account-name $STORAGE_ACCOUNT --account-key $STORAGE_KEY "
                                "--container-name test-artifacts --name $TEST_BLOB_PATH --file tests.zip && "
                                "unzip tests.zip -d tests/ && cd tests/ && "
                                "pip install -r requirements.txt && "
                                "python -m pytest --tb=short --junitxml=results.xml -v 2>&1 | tee output.log ; "
                                "cd /workspace && "
                                "az storage blob upload-batch --account-name $STORAGE_ACCOUNT --account-key $STORAGE_KEY "
                                "--destination test-artifacts --destination-path test-runs/$RUN_ID/results "
                                "--source tests/screenshots/ 2>/dev/null ; "
                                "az storage blob upload --account-name $STORAGE_ACCOUNT --account-key $STORAGE_KEY "
                                "--container-name test-artifacts --name test-runs/$RUN_ID/results.xml "
                                "--file tests/results.xml 2>/dev/null ; "
                                "az storage blob upload --account-name $STORAGE_ACCOUNT --account-key $STORAGE_KEY "
                                "--container-name test-artifacts --name test-runs/$RUN_ID/output.log "
                                "--file tests/output.log 2>/dev/null ; "
                                "echo DONE",
                            ],
                        },
                    }
                ],
                "osType": "Linux",
                "restartPolicy": "Never",
                "imageRegistryCredentials": [
                    {
                        "server": self._acr_server,
                        "username": self._acr_username,
                        "password": self._acr_password,
                    }
                ] if self._acr_server else [],
            },
        }

        # Deploy ACI
        try:
            credential = DefaultAzureCredential()
            token = credential.get_token("https://management.azure.com/.default").token
        except Exception:
            token = ""

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        url = (
            f"https://management.azure.com/subscriptions/{self._subscription_id}"
            f"/resourceGroups/{self._resource_group}/providers/Microsoft.ContainerInstance"
            f"/containerGroups/{container_name}?api-version={ACI_API_VERSION}"
        )

        logger.info("Creating ACI container: %s", container_name)
        resp = requests.put(url, headers=headers, json=aci_body, timeout=60)
        if resp.status_code not in (200, 201):
            logger.error("ACI creation failed: %s %s", resp.status_code, resp.text[:300])
            return {"status": "failed", "error": f"ACI creation failed: {resp.status_code}"}

        # Poll for completion (max 10 minutes)
        max_wait = 600
        poll_interval = 15
        start = time.time()

        while time.time() - start < max_wait:
            time.sleep(poll_interval)
            status_resp = requests.get(url, headers=headers, timeout=30)
            if status_resp.status_code == 200:
                state = status_resp.json().get("properties", {}).get("instanceView", {}).get("state", "")
                logger.info("ACI state: %s (%.0fs)", state, time.time() - start)
                if state in ("Succeeded", "Failed", "Stopped"):
                    break

        elapsed = int(time.time() - start)

        # Collect results
        results = self._collect_results(run_id)
        results["elapsed_seconds"] = elapsed
        results["container_name"] = container_name

        # Cleanup ACI
        try:
            requests.delete(url, headers=headers, timeout=30)
            logger.info("Deleted ACI container: %s", container_name)
        except Exception:
            pass

        return results

    def run_tests_local(self, generated_code: dict, app_url: str, run_id: str) -> dict:
        """Fallback: Run tests locally using subprocess (for dev/debug)."""
        import subprocess
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            # Write test files
            if generated_code.get("conftest"):
                with open(os.path.join(tmpdir, "conftest.py"), "w") as f:
                    f.write(generated_code["conftest"])

            for tf in generated_code.get("test_files", []):
                fpath = os.path.join(tmpdir, tf["filename"])
                os.makedirs(os.path.dirname(fpath), exist_ok=True)
                with open(fpath, "w") as f:
                    f.write(tf["content"])

            for po in generated_code.get("page_objects", []):
                fpath = os.path.join(tmpdir, po["filename"])
                os.makedirs(os.path.dirname(fpath), exist_ok=True)
                with open(fpath, "w") as f:
                    f.write(po["content"])

            os.makedirs(os.path.join(tmpdir, "screenshots"), exist_ok=True)

            # Run pytest
            env = os.environ.copy()
            env["APP_URL"] = app_url
            result = subprocess.run(
                ["python", "-m", "pytest", "--tb=short", "--junitxml=results.xml", "-v"],
                cwd=tmpdir,
                capture_output=True,
                text=True,
                timeout=300,
                env=env,
            )

            # Parse results
            output = result.stdout + "\n" + result.stderr
            passed = output.count(" PASSED")
            failed = output.count(" FAILED")
            errors = output.count(" ERROR")

            # Collect screenshots
            screenshots = []
            ss_dir = os.path.join(tmpdir, "screenshots")
            if os.path.exists(ss_dir):
                for fname in os.listdir(ss_dir):
                    screenshots.append(fname)

            return {
                "status": "completed",
                "passed": passed,
                "failed": failed,
                "errors": errors,
                "total": passed + failed + errors,
                "output": output[-2000:],
                "screenshots": screenshots,
                "run_id": run_id,
            }

    def _upload_blob(self, container: str, blob_name: str, data: bytes):
        """Upload data to Azure Blob Storage using DefaultAzureCredential."""
        from azure.storage.blob import BlobServiceClient

        try:
            credential = DefaultAzureCredential()
            blob_service = BlobServiceClient(
                account_url=f"https://{self._storage_account}.blob.core.windows.net",
                credential=credential,
            )
        except Exception:
            # Fallback to connection string from environment
            conn_str = os.environ.get("STORAGE_CONNECTION_STRING", "")
            if not conn_str and self._storage_account and self._storage_key:
                conn_str = (
                    f"DefaultEndpointsProtocol=https;"
                    f"AccountName={self._storage_account};"
                    f"AccountKey={self._storage_key};"
                    f"EndpointSuffix=core.windows.net"
                )
            blob_service = BlobServiceClient.from_connection_string(conn_str)

        blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
        blob_client.upload_blob(data, overwrite=True)
        logger.info("Uploaded blob: %s/%s", container, blob_name)

    def _collect_results(self, run_id: str) -> dict:
        """Download test results from blob storage."""
        try:
            base = f"https://{self._storage_account}.blob.core.windows.net/test-artifacts/test-runs/{run_id}"
            # Try to download results.xml
            resp = requests.get(f"{base}/results.xml", timeout=15)
            if resp.status_code == 200:
                return self._parse_junit_xml(resp.text, run_id)

            # Try output.log as fallback
            resp = requests.get(f"{base}/output.log", timeout=15)
            if resp.status_code == 200:
                output = resp.text
                passed = output.count(" passed")
                failed = output.count(" failed")
                return {
                    "status": "completed",
                    "passed": passed,
                    "failed": failed,
                    "total": passed + failed,
                    "output": output[-2000:],
                    "run_id": run_id,
                }
        except Exception as e:
            logger.error("Failed to collect results: %s", e)

        return {"status": "unknown", "run_id": run_id, "error": "Could not retrieve results"}

    def _parse_junit_xml(self, xml_content: str, run_id: str) -> dict:
        """Parse JUnit XML test results."""
        import xml.etree.ElementTree as ET
        try:
            root = ET.fromstring(xml_content)
            suite = root if root.tag == "testsuite" else root.find("testsuite")
            if suite is None:
                return {"status": "unknown", "run_id": run_id}

            total = int(suite.get("tests", 0))
            failures = int(suite.get("failures", 0))
            errors = int(suite.get("errors", 0))
            passed = total - failures - errors

            test_results = []
            for tc in suite.findall("testcase"):
                name = tc.get("name", "")
                classname = tc.get("classname", "")
                time_taken = float(tc.get("time", 0))
                status = "passed"
                message = ""

                failure = tc.find("failure")
                error = tc.find("error")
                if failure is not None:
                    status = "failed"
                    message = failure.get("message", "")[:500]
                elif error is not None:
                    status = "error"
                    message = error.get("message", "")[:500]

                test_results.append({
                    "name": name,
                    "classname": classname,
                    "status": status,
                    "duration": time_taken,
                    "message": message,
                })

            return {
                "status": "completed",
                "passed": passed,
                "failed": failures,
                "errors": errors,
                "total": total,
                "test_results": test_results,
                "run_id": run_id,
            }
        except Exception as e:
            return {"status": "error", "run_id": run_id, "error": str(e)}
