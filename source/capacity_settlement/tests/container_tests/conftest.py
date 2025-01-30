import os

import pytest
from testcommon.container_test import DatabricksApiClient


# this fixture is only used in CD and needs to be commented out for local testing
# local testing uses the fixture in tests/conftest.py
@pytest.fixture(scope="session")
def databricks_api_client() -> DatabricksApiClient:
    databricks_token = os.getenv("DATABRICKS_TOKEN")
    databricks_host = os.getenv("WORKSPACE_URL")
    return DatabricksApiClient(databricks_token, databricks_host)
