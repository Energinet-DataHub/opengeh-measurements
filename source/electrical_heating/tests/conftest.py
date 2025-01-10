import os
import subprocess
from pathlib import Path
from typing import Generator

import pytest
from pyspark.sql import SparkSession

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

    from telemetry_logging.logging_configuration import configure_logging

    configure_logging(
        cloud_role_name="any-cloud-role-name", tracer_name="any-tracer-name"
    )


def file_path_finder(file: str) -> str:
    return os.path.dirname(os.path.normpath(file))


@pytest.fixture(scope="session")
def source_path() -> str:
    """
    Returns the <repo-root>/source folder path.
    This function must be located in a file directly in the `tests` folder.
    """
    return file_path_finder(f"{__file__}/../..")


@pytest.fixture(scope="session")
def tests_path() -> str:
    """Returns the tests folder path."""
    return file_path_finder(f"{__file__}")


@pytest.fixture(scope="session")
def electrical_heating_path(source_path: str) -> str:
    """Returns the source/electrical_heating/ folder path."""
    return f"{source_path}/electrical_heating/src"


@pytest.fixture(scope="session")
def contracts_path(electrical_heating_path: str) -> str:
    """Returns the source/contract folder path."""
    return f"{electrical_heating_path}/contracts"


@pytest.fixture(scope="session")
def virtual_environment() -> Generator:
    """Fixture ensuring execution in a virtual environment.
    Uses `virtualenv` instead of conda environments due to problems
    activating the virtual environment from pytest."""

    # Create and activate the virtual environment
    subprocess.call(["virtualenv", ".test-pytest"])
    subprocess.call(
        "source .test-pytest/bin/activate", shell=True, executable="/bin/bash"
    )

    yield None

    # Deactivate virtual environment upon test suite tear down
    subprocess.call("deactivate", shell=True, executable="/bin/bash")


@pytest.fixture(scope="session")
def installed_package(
    virtual_environment: Generator, electrical_heating_path: str
) -> None:
    # Build the package wheel
    os.chdir(electrical_heating_path)
    subprocess.call("python -m build --wheel", shell=True, executable="/bin/bash")

    # Uninstall the package in case it was left by a cancelled test suite
    subprocess.call(
        "pip uninstall -y package",
        shell=True,
        executable="/bin/bash",
    )

    # Install wheel, which will also create console scripts for invoking the entry points of the package
    subprocess.call(
        f"pip install {electrical_heating_path}/dist/opengeh_electrical_heating-1.0-py3-none-any.whl",
        shell=True,
        executable="/bin/bash",
    )


@pytest.fixture(scope="session")
def test_session_configuration(tests_path: str) -> TestSessionConfiguration:
    settings_file_path = Path(tests_path) / "testsession.local.settings.yml"
    return TestSessionConfiguration.load(settings_file_path)
