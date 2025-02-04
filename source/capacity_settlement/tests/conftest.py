from typing import Generator

import pytest
from pyspark.sql import SparkSession
from telemetry_logging.logging_configuration import configure_logging

from tests import PROJECT_ROOT
from tests.testsession_configuration import TestSessionConfiguration


@pytest.fixture(scope="module", autouse=True)
def clear_cache(spark: SparkSession) -> Generator[None, None, None]:
    yield
    # Clear the cache after each test module to avoid memory issues
    spark.catalog.clearCache()


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    session = SparkSession.builder.appName("testcommon").getOrCreate()
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def configure_dummy_logging() -> None:
    """Ensure that logging hooks don't fail due to _TRACER_NAME not being set."""
    configure_logging(cloud_role_name="any-cloud-role-name", tracer_name="any-tracer-name")


@pytest.fixture(scope="session")
def contracts_path() -> str:
    """
    Returns the source/contract folder path.
    Please note that this only works if current folder haven't been changed prior using
    `os.chdir()`. The correctness also relies on the prerequisite that this function is
    actually located in a file located directly in the tests folder.
    """
    return f"{PROJECT_ROOT}/contracts"


@pytest.fixture(scope="session")
def test_session_configuration() -> TestSessionConfiguration:  # noqa: F821
    settings_file_path = PROJECT_ROOT / "tests" / "testsession.local.settings.yml"
    return TestSessionConfiguration.load(settings_file_path)
