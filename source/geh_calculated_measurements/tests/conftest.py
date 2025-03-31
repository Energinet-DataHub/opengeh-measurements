### This file contains the fixtures that are used in the tests. ###
import os
from typing import Generator

import geh_common.telemetry.logging_configuration
import pytest
from delta import configure_spark_with_delta_pip
from geh_common.telemetry.logging_configuration import LoggingSettings, configure_logging
from geh_common.testing.dataframes import AssertDataframesConfiguration, configure_testing
from pyspark.sql import SparkSession

from tests import TESTS_ROOT, create_job_environment_variables
from tests.testsession_configuration import TestSessionConfiguration


@pytest.fixture(scope="session")
def dummy_logging() -> Generator[None, None, None]:
    """Ensure that logging hooks don't fail due to _TRACER_NAME not being set."""
    env_args = create_job_environment_variables()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(os, "environ", env_args)
        mp.setattr(geh_common.telemetry.logging_configuration, "configure_azure_monitor", lambda *args, **kwargs: None)
        mp.setattr(geh_common.telemetry.logging_configuration, "get_is_instrumented", lambda *args, **kwargs: False)
        settings = LoggingSettings(cloud_role_name="test_role", subsystem="test_subsystem")
        configure_logging(logging_settings=settings)
        yield


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
