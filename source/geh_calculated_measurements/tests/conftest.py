### This file contains the fixtures that are used in the tests. ###
from typing import Generator
from unittest import mock

import geh_common.telemetry.logging_configuration as config
import pytest
from delta import configure_spark_with_delta_pip
from geh_common.testing.dataframes import AssertDataframesConfiguration, configure_testing
from pyspark.sql import SparkSession

from geh_calculated_measurements.database_migrations.migrations_runner import migrate
from tests import TESTS_ROOT
from tests.testsession_configuration import TestSessionConfiguration


@pytest.fixture(scope="session")
def env_args_fixture_logging() -> dict[str, str]:
    env_args = {
        "CLOUD_ROLE_NAME": "test_role",
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "connection_string",
        "SUBSYSTEM": "test_subsystem",
        "CATALOG_NAME": "spark_catalog",
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
def configure_dummy_logging(env_args_fixture_logging, script_args_fixture_logging) -> Generator[None, None, None]:
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


@pytest.fixture(scope="session")
def test_session_configuration() -> TestSessionConfiguration:
    """Load the test session configuration from the testsession.local.settings.yml file.

    This is a useful feature for developers who wants to run the tests with different configurations
    on their local machine. The file is not included in the repository, so it's up to the developer to create it.
    """
    settings_file_path = TESTS_ROOT / "testsession.local.settings.yml"
    return TestSessionConfiguration.load(settings_file_path)


@pytest.fixture(scope="session", autouse=True)
def _configure_testing_decorator(test_session_configuration: TestSessionConfiguration) -> None:
    configure_testing(
        is_testing=test_session_configuration.scenario_tests.testing_decorator_enabled,
        rows=test_session_configuration.scenario_tests.testing_decorator_max_rows,
    )


@pytest.fixture(scope="session")
def assert_dataframes_configuration(
    test_session_configuration: TestSessionConfiguration,
) -> AssertDataframesConfiguration:
    """This fixture is used for comparing data frames in scenario tests.

    It's mainly specific to the scenario tests. The fixture is placed here to avoid code duplication."""
    return AssertDataframesConfiguration(
        show_actual_and_expected_count=test_session_configuration.scenario_tests.show_actual_and_expected_count,
        show_actual_and_expected=test_session_configuration.scenario_tests.show_actual_and_expected,
        show_columns_when_actual_and_expected_are_equal=test_session_configuration.scenario_tests.show_columns_when_actual_and_expected_are_equal,
    )


def _create_databases(spark: SparkSession) -> None:
    # """
    # Create Unity Catalog databases as they are not created by migration scripts.
    # They are created by infrastructure (in the real environments)
    # In tests they are created in the single available default catalog.
    # """
    spark.sql("CREATE DATABASE IF NOT EXISTS measurements_calculated")
    spark.sql("CREATE DATABASE IF NOT EXISTS measurements_calculated_internal")


@pytest.fixture(scope="session")
def migrations_executed(spark: SparkSession) -> None:
    """Executes all migrations.

    This fixture is useful for all tests that require the migrations to be executed. E.g. when
    a view/dataprodcut/table is required."""
    _create_databases(spark)
    migrate()
