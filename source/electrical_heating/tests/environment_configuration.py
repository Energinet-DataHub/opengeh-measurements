from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from tests import PROJECT_ROOT


class EnvironmentConfiguration(BaseSettings):
    databricks_token: str = Field(alias="DATABRICKS_TOKEN")
    databricks_workspace_url: str = Field(alias="DATABRICKS_WORKSPACE_URL")

    model_config = SettingsConfigDict(
        env_file=f"{PROJECT_ROOT}/tests/.env",
        env_file_encoding="utf-8",
    )
