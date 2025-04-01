### This file contains the fixtures that are used in the tests. ###

import pytest

from tests import SPARK_CATALOG_NAME


@pytest.fixture(scope="module")
def dummy_env_args() -> dict[str, str]:
    env_args = {
        "CLOUD_ROLE_NAME": "test_role",
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "connection_string",
        "SUBSYSTEM": "test_subsystem",
        "CATALOG_NAME": SPARK_CATALOG_NAME,
    }
    return env_args
