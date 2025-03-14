from typing import Generator

import pytest
from pyspark.sql import SparkSession

from tests import PROJECT_ROOT, TESTS_ROOT

CONTAINER_NAME = "missing_measurements_log"


@pytest.fixture(scope="module", autouse=True)
def clear_cache(spark: SparkSession) -> Generator[None, None, None]:
    """
    Clear the cache after each test module to avoid memory issues.
    """
    yield
    spark.catalog.clearCache()


@pytest.fixture(scope="session")
def tests_path() -> str:
    """Returns the tests folder path."""
    return (TESTS_ROOT / CONTAINER_NAME).as_posix()


@pytest.fixture(scope="session")
def contracts_path() -> str:
    """Returns the source/contract folder path."""
    return (PROJECT_ROOT / "src" / "geh_calculated_measurements" / CONTAINER_NAME / "contracts").as_posix()
