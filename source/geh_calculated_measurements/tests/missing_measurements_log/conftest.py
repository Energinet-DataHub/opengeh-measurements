### This file contains the fixtures that are used in the tests. ###

import pytest


@pytest.fixture(scope="module")
def dummy_env_args() -> dict[str, str]:
    env_args = {
        "CLOUD_ROLE_NAME": "test_role",
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "connection_string",
        "SUBSYSTEM": "test_subsystem",
        "CATALOG_NAME": "spark_catalog",
    }
    return env_args
