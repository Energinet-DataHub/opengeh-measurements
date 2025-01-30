from pathlib import Path

import pytest
from pydantic_settings import BaseSettings
from testcommon.container_test import DatabricksApiClient

PROJECT_ROOT = Path(__file__).parent.parent.parent


class ContainerSettings(BaseSettings):
    DATABRICKS_TOKEN: str
    WORKSPACE_URL: str

    class Config:
        env_file = f"{PROJECT_ROOT}/tests/container_tests/.env"
        env_file_encoding = "utf-8"


@pytest.fixture(scope="session")
def databricks_api_client() -> DatabricksApiClient:
    container_settings = ContainerSettings()
    databricksApiClient = DatabricksApiClient(container_settings.DATABRICKS_TOKEN, container_settings.WORKSPACE_URL)
    return databricksApiClient
