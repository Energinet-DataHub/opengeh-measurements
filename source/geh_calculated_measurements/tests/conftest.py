### This file contains the fixtures that are used in the tests. ###
import os
from typing import Generator
from unittest import mock

import geh_common.telemetry.logging_configuration as config
import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def env_args_fixture_logging() -> dict[str, str]:
    env_args = {
        "CLOUD_ROLE_NAME": "test_role",
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "connection_string",
        "SUBSYSTEM": "test_subsystem",
    }
    return env_args


@pytest.fixture(scope="session")
def script_args_fixture_logging() -> list[str]:
    sys_argv = [
        "program_name",
        "--orchestration-instance-id",
        "00000000-0000-0000-0000-000000000001",
    ]
    return sys_argv


@pytest.fixture(scope="session", autouse=True)
def configure_dummy_logging(env_args_fixture_logging, script_args_fixture_logging)  -> Generator[None, None, None]:
    """Ensure that logging hooks don't fail due to _TRACER_NAME not being set."""
    with (
        mock.patch("sys.argv", script_args_fixture_logging),
        mock.patch.dict("os.environ", env_args_fixture_logging, clear=False),
        mock.patch(
            "geh_common.telemetry.logging_configuration.configure_azure_monitor"
        ),  # Patching call to configure_azure_monitor in order to not actually connect to app. insights.
    ):
        logging_settings = config.LoggingSettings()
        yield config.configure_logging(logging_settings=logging_settings, extras=None)


@pytest.fixture(scope="module", autouse=True)
def clear_cache(spark: SparkSession) -> Generator[None, None, None]:
    """
    Clear the cache after each test module to avoid memory issues.
    """
    yield
    spark.catalog.clearCache()


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Create a Spark session with Delta Lake enabled.
    """
    session = (
        SparkSession.builder.appName("geh_calculated_measurements")  # # type: ignore
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Enable Hive support for persistence across test sessions
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
    )
    session = configure_spark_with_delta_pip(session).getOrCreate()
    yield session
    session.stop()


# https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_collection_modifyitems
def pytest_collection_modifyitems(config, items) -> None:
    env_file_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_file_path):
        skip_subsystem_tests = pytest.mark.skip(
            reason="Skipping subsystem tests because .env file is missing. See .sample.env for an example."
        )
        for item in items:
            if "subsystem_tests" in item.nodeid:
                item.add_marker(skip_subsystem_tests)
