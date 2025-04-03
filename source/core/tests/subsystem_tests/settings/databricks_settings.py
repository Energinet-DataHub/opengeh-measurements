from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).parent.parent


class DatabricksSettings(BaseSettings):
    """
    Contains the environment configuration for the tests.
    This class must be included when running tests in CD.
    """

    workspace_url: str = Field(alias="DATABRICKS_WORKSPACE_URL")
    token: str = Field(alias="DATABRICKS_TOKEN")
    warehouse_id: str = Field(alias="DATABRICKS_WAREHOUSE_ID")
    catalog: str = Field(alias="CATALOG")

    model_config = SettingsConfigDict(
        env_file=f"{PROJECT_ROOT}/.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
