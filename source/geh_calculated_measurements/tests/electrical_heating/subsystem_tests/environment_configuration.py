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
    workspace_id: str = Field(alias="WORKSPACE_ID")

    azure_client_id: str = Field(alias="AZURE_CLIENT_ID")
    azure_tenant_id: str = Field(alias="AZURE_TENANT_ID")
    azure_client_secret: str = Field(alias="AZURE_CLIENT_SECRET")
    azure_subscription_id: str = Field(alias="AZURE_SUBSCRIPTION_ID")
    azure_keyvault_url: str = Field(alias="AZURE_KEYVAULT_URL")

    model_config = SettingsConfigDict(
        env_file=f"{PROJECT_ROOT}/.env",
        env_file_encoding="utf-8",
    )
