import os
from enum import Enum
from typing import Any


class EnvironmentVariable(Enum):
    CATALOG_NAME = "CATALOG_NAME"
    DATALAKE_STORAGE_ACCOUNT = "DATALAKE_STORAGE_ACCOUNT"
    APPLICATIONINSIGHTS_CONNECTION_STRING = "APPLICATIONINSIGHTS_CONNECTION_STRING"


def get_applicationinsights_connection_string() -> str:
    return get_env_variable_or_throw(EnvironmentVariable.APPLICATIONINSIGHTS_CONNECTION_STRING)


def get_catalog_name() -> str:
    return get_env_variable_or_throw(EnvironmentVariable.CATALOG_NAME)


def get_datalake_storage_account() -> str:
    return get_env_variable_or_throw(EnvironmentVariable.DATALAKE_STORAGE_ACCOUNT)


def get_env_variable_or_throw(variable: EnvironmentVariable) -> Any:
    env_variable = os.getenv(variable.name)
    if env_variable is None:
        raise ValueError(f"Environment variable not found: {variable.name}")

    return env_variable
