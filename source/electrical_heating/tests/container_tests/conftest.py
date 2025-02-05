import os

import pytest
from testcommon.container_test import DatabricksApiClient


# For the test to be started from your local machine, you need to set the environment variables in a .env file.
@pytest.fixture(scope="session")
def databricks_api_client() -> DatabricksApiClient:
    databricks_token = os.getenv("DATABRICKS_TOKEN")
    databricks_host = os.getenv("WORKSPACE_URL")
    return DatabricksApiClient(databricks_token, databricks_host)
