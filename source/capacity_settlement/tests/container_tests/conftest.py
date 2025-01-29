import os

import pytest
from testcommon.container_test import DatabricksApiClient


@pytest.fixture(scope="session")
def databricks_api_client() -> DatabricksApiClient:
    databricks_token = os.getenv("DATABRICKS_TOKEN")
    databricks_host = os.getenv("WORKSPACE_URL")
    return DatabricksApiClient(databricks_token, databricks_host)