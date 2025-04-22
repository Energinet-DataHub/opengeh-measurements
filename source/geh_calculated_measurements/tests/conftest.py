import os
import shutil
import sys
from typing import Generator
from unittest import mock

import geh_common.telemetry.logging_configuration
import pytest
from filelock import FileLock
from geh_common.data_products.electricity_market_measurements_input import (
    missing_measurements_log_metering_point_periods_v1,
    net_consumption_group_6_child_metering_points_v1,
    net_consumption_group_6_consumption_metering_point_periods_v1,
)
from geh_common.telemetry.logging_configuration import configure_logging
from geh_common.testing.dataframes import AssertDataframesConfiguration, configure_testing
from geh_common.testing.delta_lake.delta_lake_operations import create_database, create_table
from geh_common.testing.spark.spark_test_session import get_spark_test_session
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsDatabaseDefinition
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from geh_calculated_measurements.common.infrastructure.electricity_market import (
    DEFAULT_ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME,
)
from geh_calculated_measurements.database_migrations import MeasurementsCalculatedInternalDatabaseDefinition
from geh_calculated_measurements.database_migrations.migrations_runner import _migrate
from geh_calculated_measurements.missing_measurements_log.infrastructure import MeteringPointPeriodsTable
from tests import (
    SPARK_CATALOG_NAME,
    TESTS_ROOT,
    create_job_environment_variables,
)
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.testsession_configuration import TestSessionConfiguration


# https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_collection_modifyitems
def pytest_collection_modifyitems(config, items) -> None:
    skip_subsystem_tests = pytest.mark.skip(
        reason="Skipping subsystem tests because environmental variables could not be set. See .sample.env file on how to set environmental variables locally"
    )
    try:
        EnvironmentConfiguration()
    except Exception:
        for item in items:
            if "subsystem_tests" in item.nodeid:
                item.add_marker(skip_subsystem_tests)
            if "integration" in item.nodeid:
                item.add_marker(skip_subsystem_tests)


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
def spark(tmp_path_factory, worker_id) -> Generator[SparkSession, None, None]:
    """
    Create a Spark session with Delta Lake enabled.
    """
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


@pytest.fixture(scope="session")
def migrations_executed(spark: SparkSession) -> None:
    """Executes all migrations.

    This fixture is useful for all tests that require the migrations to be executed. E.g. when
    a view/dataprodcut/table is required."""

    # Databases are created in dh3infrastructure using terraform
    # So we need to create them in test environment
    for db in [
        MeasurementsCalculatedInternalDatabaseDefinition.measurements_calculated_internal_database,
        CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME,
    ]:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    _migrate(SPARK_CATALOG_NAME)


@pytest.fixture(scope="session")
def external_dataproducts_created(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory, worker_id) -> None:
    """Create external dataproducts (databases, tables and views) as needed by tests."""
    if worker_id == "master":
        # not executing with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        return _create_dataproducts(spark)

    # get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    fn = root_tmp_dir / "data.txt"
    with FileLock(str(fn) + ".lock"):
        if fn.is_file():
            return
        else:
            _create_dataproducts(spark)
            fn.write_text("done", encoding="utf-8")


def _create_dataproducts(spark):
    # Create measurements gold database and tables
    create_database(spark, MeasurementsGoldDatabaseDefinition.DATABASE_NAME)
    create_table(
        spark,
        database_name=MeasurementsGoldDatabaseDefinition.DATABASE_NAME,
        table_name=MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS,
        schema=CurrentMeasurements.schema,
    )

    # Create missing measurements log database and tables
    create_database(spark, DEFAULT_ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME)
    create_table(
        spark,
        database_name=DEFAULT_ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME,
        table_name=missing_measurements_log_metering_point_periods_v1.view_name,
        schema=MeteringPointPeriodsTable.schema,
    )

    # Create net consumption group 6 database and tables
    create_table(
        spark,
        database_name=DEFAULT_ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME,
        table_name=net_consumption_group_6_consumption_metering_point_periods_v1.view_name,
        schema=net_consumption_group_6_consumption_metering_point_periods_v1.schema,
    )
    create_table(
        spark,
        database_name=DEFAULT_ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME,
        table_name=net_consumption_group_6_child_metering_points_v1.view_name,
        schema=net_consumption_group_6_child_metering_points_v1.schema,
    )
