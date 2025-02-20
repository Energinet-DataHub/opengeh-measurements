from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from tests import PROJECT_ROOT


class EnvironmentConfiguration(BaseSettings):
    """
    Contains the environment configuration for the tests.
    This class must be included when running tests in CD.
    """

    databricks_token: str = Field(alias="DATABRICKS_TOKEN")
    workspace_url: str = Field(alias="WORKSPACE_URL")
    shared_keyvault_name: str = Field(alias="SHARED_KEYVAULT_NAME")

    model_config = SettingsConfigDict(
        env_file=f"{PROJECT_ROOT}/tests/.env",
        env_file_encoding="utf-8",
    )
