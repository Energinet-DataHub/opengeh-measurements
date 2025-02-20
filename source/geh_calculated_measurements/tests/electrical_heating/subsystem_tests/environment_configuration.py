from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).parent.parent.parent


class EnvironmentConfiguration(BaseSettings):
    """
    Contains the environment configuration for the tests.
    This class must be included when running tests in CD.
    """

    databricks_token: str = Field(alias="DATABRICKS_TOKEN")
    workspace_url: str = Field(alias="WORKSPACE_URL")
    core_workspace_id: str = Field(alias="CORE_WORKSPACE_ID")
    shared_keyvault_name: str = Field(alias="SHARED_KEYVAULT_NAME")

    model_config = SettingsConfigDict(
        env_file=f"{PROJECT_ROOT}/.env",
        env_file_encoding="utf-8",
    )
