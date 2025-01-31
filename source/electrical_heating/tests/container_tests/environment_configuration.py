from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).parent.parent.parent


class EnvironmentConfiguration(BaseSettings):
    """This class resides inside the container test folder because it is needed by the test framework.
    If placed outside the container test folder it is not visible to the test framework."""

    databricks_token: str = Field(alias="DATABRICKS_TOKEN")
    databricks_workspace_url: str = Field(alias="DATABRICKS_WORKSPACE_URL")

    model_config = SettingsConfigDict(
        env_file=f"{PROJECT_ROOT}/tests/.env",
        env_file_encoding="utf-8",
    )
