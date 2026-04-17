import os
from dataclasses import dataclass


class ConfigError(Exception):
    """Raised when required configuration is missing."""


def _require_env(key: str) -> str:
    """Get a required environment variable or raise a clear error."""
    value = os.environ.get(key)
    if not value:
        raise ConfigError(
            f"Required environment variable '{key}' is not set. "
            f"Check Function App settings or local.settings.json."
        )
    return value


@dataclass(frozen=True)
class AppConfig:
    environment: str
    key_vault_uri: str
    openai_endpoint: str
    openai_deployment: str
    synapse_endpoint: str
    synapse_database: str
    source_db_server: str
    source_db_name: str
    storage_account_name: str
    ado_org: str
    ado_project: str
    ado_repo: str
    config_db_server: str
    config_db_name: str

    @classmethod
    def from_env(cls) -> "AppConfig":
        source_server = _require_env("SOURCE_DB_SERVER")
        source_db = _require_env("SOURCE_DB_NAME")
        return cls(
            environment=os.environ.get("ENVIRONMENT", "dev"),
            key_vault_uri=_require_env("KEY_VAULT_URI"),
            openai_endpoint=_require_env("AZURE_OPENAI_ENDPOINT"),
            openai_deployment=os.environ.get("AZURE_OPENAI_DEPLOYMENT", "Phi-4"),
            synapse_endpoint=_require_env("SYNAPSE_SQL_ENDPOINT"),
            synapse_database=os.environ.get("SYNAPSE_SQL_DATABASE", "bipool"),
            source_db_server=source_server,
            source_db_name=source_db,
            storage_account_name=_require_env("STORAGE_ACCOUNT_NAME"),
            ado_org=os.environ.get("ADO_ORG", ""),
            ado_project=os.environ.get("ADO_PROJECT", ""),
            ado_repo=os.environ.get("ADO_REPO", ""),
            config_db_server=os.environ.get("CONFIG_DB_SERVER", source_server),
            config_db_name=os.environ.get("CONFIG_DB_NAME", source_db),
        )
