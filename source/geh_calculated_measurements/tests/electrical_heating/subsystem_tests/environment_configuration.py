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
    shared_keyvault_name: str = Field(alias="SHARED_KEYVAULT_NAME")

    catalog_name: str = Field(alias="CATALOG")
    schema_name: str = Field(alias="SCHEMA")

    time_series_points_table: str = Field(alias="TIME_SERIES_POINTS")
    consumption_points_table: str = Field(alias="CONSUMPTION_METERING_POINTS")
    child_points_table: str = Field(alias="CHILD_METERING_POINTS")

    model_config = SettingsConfigDict(env_file=f"{PROJECT_ROOT}/tests/.env", env_file_encoding="utf-8", extra="ignore")
