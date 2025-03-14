### This file contains the fixtures that are used in the tests. ###
from pathlib import Path
from typing import Generator

import pytest
from pyspark.sql import SparkSession

from tests import PROJECT_ROOT, TESTS_ROOT
from tests.testsession_configuration import TestSessionConfiguration


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
    return (TESTS_ROOT / "electrical_heating").as_posix()


@pytest.fixture(scope="session")
def contracts_path() -> str:
    """Returns the source/contract folder path."""
    return (PROJECT_ROOT / "src" / "geh_calculated_measurements" / "electrical_heating" / "contracts").as_posix()
