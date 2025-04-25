from typing import Any

from geh_common.application.settings import ApplicationSettings
from pydantic import Field, model_validator
from pydantic_settings import SettingsConfigDict

from geh_calculated_measurements.common.infrastructure.electricity_market import (
    DEFAULT_ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME,
)
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
    shared_keyvault_name: str = Field(init=False, default="", alias="SHARED_KEYVAULT_NAME")
    shared_keyvault_url: str = Field(init=False, default="", alias="SHARED_KEYVAULT_URL")
    azure_log_analytics_workspace_id_secret_name: str = Field(
        init=False, default="log-shared-workspace-id", alias="SHARED_AZURE_LOG_ANALYTICS_WORKSPACE_ID"
    )

    # For dev enviroments this is overriden by environment variable in dh3environments. It will be a name dedicated
    # for calculated measurements subsystem tests to create a "mock" of the Electricity Market data products.
    electricity_market_database_name: str = Field(
        init=False,
        alias="ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME",
        default=DEFAULT_ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME,
    )

    # for performance test

    schema_name: str = Field(init=False, default="", alias="SHARED_SCHEMA_NAME")

    time_series_points_table: str = Field(init=False, default="", alias="TIME_SERIES_POINTS_TABLE")
    consumption_metering_points_table: str = Field(init=False, default="", alias="CONSUMPTION_METERING_POINTS_TABLE")
    child_metering_points_table: str = Field(init=False, default="", alias="CHILD_METERING_POINTS_TABLE")

    model_config = SettingsConfigDict(
        env_file=f"{TESTS_ROOT}/.env",
        env_file_encoding="utf-8",
    )

    @model_validator(mode="after")
    def set_shared_keyvault_url(self) -> Any:
        if self.shared_keyvault_name and self.shared_keyvault_url:
            raise ValueError("Both SHARED_KEYVAULT_NAME and SHARED_KEYVAULT_URL are set. Please set only one.")
        elif not self.shared_keyvault_name and not self.shared_keyvault_url:
            raise ValueError("Either SHARED_KEYVAULT_NAME or SHARED_KEYVAULT_URL must be set.")
        if self.shared_keyvault_name and not self.shared_keyvault_url:
            self.shared_keyvault_url = f"https://{self.shared_keyvault_name}.vault.azure.net/"
        return self
