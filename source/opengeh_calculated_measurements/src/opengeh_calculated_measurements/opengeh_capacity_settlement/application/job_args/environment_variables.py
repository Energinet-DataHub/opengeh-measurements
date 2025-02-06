import os
from enum import Enum
from typing import Any


# TODO: Move to shared library
class EnvironmentVariable(Enum):
    CATALOG_NAME = "CATALOG_NAME"


def get_catalog_name() -> str:
    return get_env_variable_or_throw(EnvironmentVariable.CATALOG_NAME)


def get_env_variable_or_throw(variable: EnvironmentVariable) -> Any:
    env_variable = os.getenv(variable.name)
    if env_variable is None:
        raise ValueError(f"Environment variable not found: {variable.name}")

    return env_variable
