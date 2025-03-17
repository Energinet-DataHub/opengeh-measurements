from geh_common.application.settings import ApplicationSettings
from pydantic import Field
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

    catalog_name: str = Field(alias="SHARED_CATALOG_NAME")

    model_config = SettingsConfigDict(
        env_file=f"{TESTS_ROOT}/.env",
        env_file_encoding="utf-8",
    )
