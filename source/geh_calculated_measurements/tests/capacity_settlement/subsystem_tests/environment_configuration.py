from pathlib import Path

from geh_common.application.settings import ApplicationSettings
from pydantic_settings import SettingsConfigDict

PROJECT_ROOT = Path(__file__).parent.parent.parent


class EnvironmentConfiguration(ApplicationSettings):
    """
    Contains the environment configuration for the tests.
    This class must be included when running tests in CD.
    """

    warehouse_id: str = "CORE_WAREHOUSE_ID"
    catalog_name: str = "SHARED_CATALOG_NAME"

    databricks_token: str = "DATABRICKS_TOKEN"
    workspace_url: str = "WORKSPACE_URL"
    shared_keyvault_name: str = "SHARED_KEYVAULT_NAME"

    model_config = SettingsConfigDict(
        env_file=f"{PROJECT_ROOT}/.env",
        env_file_encoding="utf-8",
    )
