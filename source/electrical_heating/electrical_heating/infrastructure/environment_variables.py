import os
from typing import Any


# TODO: Move to shared library
class EnvironmentVariable:
    CATALOG_NAME = "CATALOG_NAME"
    TIME_ZONE = "TIME_ZONE"


def get_catalog_name() -> str:
    return get_env_variable_or_throw(EnvironmentVariable.CATALOG_NAME)


def get_time_zone() -> str:
    return get_env_variable_or_throw(EnvironmentVariable.TIME_ZONE)


def get_env_variable_or_throw(variable: str) -> Any:
    env_variable = os.getenv(variable)
    if env_variable is None:
        raise ValueError(f"Environment variable not found: {variable}")

    return env_variable
