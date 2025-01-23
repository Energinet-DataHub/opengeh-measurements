# This will contain methods that should be put into a 3rd party library, and reused across multiple projects.
import os
from enum import Enum
from typing import Any
from pyspark import SparkConf
from pyspark.sql.session import SparkSession


def get_storage_base_path(
    storage_account_name: str,
    container_name: str,
) -> str:
    return f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"


def get_checkpoint_path(
    datalake_storage_account_name: str,
    container_name: str,
    table_name: str,
) -> str:
    return (
        get_storage_base_path(datalake_storage_account_name, container_name)
        + f"checkpoints/{table_name}"
    )


def get_full_table_name(database: str, table: str) -> str:
    return f"{database}.{table}"


class EnvironmentVariable(Enum):
    DATALAKE_STORAGE_ACCOUNT = "DATALAKE_STORAGE_ACCOUNT"


def get_env_variable_or_throw(variable: EnvironmentVariable) -> Any:
    env_variable = os.getenv(variable.name)
    if env_variable is None:
        raise ValueError(f"Environment variable not found: {variable.name}")

    return env_variable

