### This file contains the fixtures that are used in the tests. ###
import os
import shutil
import sys
from typing import Generator
from unittest import mock

import geh_common.telemetry.logging_configuration
import pytest
from geh_common.telemetry.logging_configuration import configure_logging
from geh_common.testing.dataframes import AssertDataframesConfiguration, configure_testing
from geh_common.testing.spark.spark_test_session import get_spark_test_session
from pyspark.sql import SparkSession

from tests import TESTS_ROOT, create_job_environment_variables
from tests.testsession_configuration import TestSessionConfiguration


@pytest.fixture(scope="module")
def dummy_logging() -> Generator[None, None, None]:
    """Ensure that logging hooks don't fail due to _TRACER_NAME not being set.

    This fixture effectively disables the telemetry logging to Azure."""
    env_args = create_job_environment_variables()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(os, "environ", env_args)
        mp.setattr(geh_common.telemetry.logging_configuration, "configure_azure_monitor", lambda *args, **kwargs: None)
        mp.setattr(geh_common.telemetry.logging_configuration, "get_is_instrumented", lambda *args, **kwargs: False)
        configure_logging(cloud_role_name="test_role", subsystem="test_subsystem")
        yield


@pytest.fixture(scope="module", autouse=True)
def clear_cache(spark: SparkSession) -> Generator[None, None, None]:
    """
    Clear the cache after each test module to avoid memory issues.
    """
    yield
    spark.catalog.clearCache()


# pytest-xdist plugin does not work with SparkSession as a fixture. The session scope is not supported.
# Therefore, we need to create a global variable to store the Spark session and data directory.
# This is a workaround to avoid creating a new Spark session for each test.
_spark, data_dir = get_spark_test_session()


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Create a Spark session with Delta Lake enabled.
    """
    _spark.sparkContext.setLogLevel("ERROR")
    yield _spark
    _spark.stop()
    shutil.rmtree(data_dir)


@pytest.fixture(scope="session", autouse=True)
def fix_print():
    """
    pytest-xdist disables stdout capturing by default, which means that print() statements
    are not captured and displayed in the terminal.
    That's because xdist cannot support -s for technical reasons wrt the process execution mechanism
    https://github.com/pytest-dev/pytest-xdist/issues/354
    """
    original_print = print
    with mock.patch("builtins.print") as mock_print:
        mock_print.side_effect = lambda *args, **kwargs: original_print(*args, **{"file": sys.stderr, **kwargs})
        yield mock_print


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
