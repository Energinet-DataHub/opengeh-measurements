from typing import Any

from geh_common.application.settings import ApplicationSettings
from pydantic import Field, model_validator
from pydantic_settings import SettingsConfigDict

from tests import TESTS_ROOT


class EnvironmentConfiguration(ApplicationSettings):
    """
    Contains the environment configuration for the tests.
    This class must be included when running tests in CD.
    """

    warehouse_id: str = Field(init=False, alias="CALCULATED_MEASUREMENTS_WAREHOUSE_ID")
    catalog_name: str = Field(init=False, alias="SHARED_CATALOG_NAME")

    databricks_token: str = Field(init=False, alias="DATABRICKS_TOKEN")
    workspace_url: str = Field(init=False, alias="WORKSPACE_URL")
    shared_keyvault_name: str = Field(init=False, alias="SHARED_KEYVAULT_NAME")
    shared_keyvault_url: str = Field(init=False, default="", alias="SHARED_KEYVAULT_URL")

    # for performance test

    schema_name: str = Field(init=False, default="", alias="SHARED_SCHEMA_NAME")

    time_series_points_table: str = Field(init=False, default="", alias="TIME_SERIES_POINTS_TABLE")
    consumption_metering_points_table: str = Field(init=False, default="", alias="CONSUMPTION_METERING_POINTS_TABLE")
    child_metering_points_table: str = Field(init=False, default="", alias="CHILD_METERING_POINTS_TABLE")

    model_config = SettingsConfigDict(
        env_file=f"{TESTS_ROOT}/.env",
        env_file_encoding="utf-8",
    )

    @model_validator(mode="before")
    @classmethod
    def set_shared_keyvault_url(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "shared_keyvault_url" not in data and "shared_keyvault_name" in data:
                data["shared_keyvault_url"] = f"https://{data['shared_keyvault_name']}.vault.azure.net/"
        return data
