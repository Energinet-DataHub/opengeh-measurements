import os
import shutil
import sys
from collections import namedtuple
from pathlib import Path
from typing import Generator
from unittest import mock

import geh_common.telemetry.logging_configuration
import pytest
from delta import configure_spark_with_delta_pip
from geh_common.data_products.electricity_market_measurements_input import (
    missing_measurements_log_metering_point_periods_v1,
    net_consumption_group_6_child_metering_points_v1,
    net_consumption_group_6_consumption_metering_point_periods_v1,
)
from geh_common.telemetry.logging_configuration import configure_logging
from geh_common.testing.dataframes import AssertDataframesConfiguration, configure_testing
from geh_common.testing.delta_lake.delta_lake_operations import create_database, create_table
from pyspark import SparkConf
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

TestingContext = namedtuple("TestingContext", "data_dir")
SparkTestingContext = namedtuple("SparkTestingContext", ["test_ctx", "spark"])

delta_core_jar = str(Path(__file__).parent / "delta-core_2.13-3.3.0.jar")


def mock_test_objects():
    pass


def prepare_data():
    data_dir = Path(__file__).parent / "data"
    return str(data_dir.resolve())


def get_spark_session(tmp_path_factory, extra_packages: list[str] | None = None) -> SparkSession:
    extra_java_options = [
        # reduces the memory footprint by limiting the Delta log cache
        "-Ddelta.log.cacheSize=3",
        # allows the JVM garbage collector to remove unused classes that Spark generates a lot of dynamically
        "-XX:+CMSClassUnloadingEnabled",
        # tells JVM to use 32-bit addresses instead of 64 (If you’re not planning to use more than 32G of RAM)
        "-XX:+UseCompressedOops",
        # When running locally, Spark uses the Apache Derby database. This database uses RAM and the local disc to store files.
        # Using the same metastore in all processes can lead to concurrent modifications of the same table metadata.
        # This can lead to errors (e.g. unknown partitions) and undesired interference between tests.
        # To avoid it, use a separate metastore in each process.
        f"-Dderby.system.home={str(tmp_path_factory.mktemp('derby').resolve())}",
    ]

    config = {
        "spark.driver.extraJavaOptions": " ".join(extra_java_options),
        "spark.sql.shuffle.partitions": "1",
        "spark.databricks.delta.snapshotPartitions": "2",
        "spark.ui.showConsoleProgress": "false",
        "spark.ui.enabled": "false",
        "spark.ui.dagGraph.retainedRootRDDs": "1",
        "spark.ui.retainedJobs": "1",
        "spark.ui.retainedStages": "1",
        "spark.ui.retainedTasks": "1",
        "spark.sql.ui.retainedExecutions": "1",
        "spark.worker.ui.retainedExecutors": "1",
        "spark.worker.ui.retainedDrivers": "1",
        "spark.driver.memory": "2g",
        "spark.sql.session.timeZone": "UTC",
        # Do not write Spark outputs from different processes to the same output folder,
        # as this can lead to data races and test interference.
        # Use tmp_path_factory to set up the output folder in the session-scoped fixture.
        "spark.sql.warehouse.dir": str(tmp_path_factory.mktemp("warehouse").resolve()),
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # "spark.jars"  : f"file://{delta_core_jar}",
    }

    conf = SparkConf().setAll(pairs=[(k, v) for k, v in config.items()])

    # Create the Spark session
    builder = configure_spark_with_delta_pip(
        SparkSession.Builder().master("local[1]").config(conf=conf), extra_packages=extra_packages
    )
    session = builder.getOrCreate()
    session.sparkContext.setCheckpointDir(str(tmp_path_factory.mktemp("checkpoints").resolve()))
    return session


@pytest.fixture(scope="session", autouse=True)
def testing_context(tmp_path_factory) -> Generator[TestingContext, None, None]:
    mock_test_objects()
    data_dir = tmp_path_factory.mktemp("data_dir")

    yield TestingContext(str(data_dir.resolve()))
    shutil.rmtree(data_dir)


@pytest.fixture(scope="session")
def spark_test_session(testing_context, tmp_path_factory):
    spark = get_spark_session(tmp_path_factory)
    yield SparkTestingContext(testing_context, spark)
    spark.stop()


@pytest.fixture(scope="session")
def spark(spark_test_session) -> SparkSession:
    """
    Create a Spark session with Delta Lake enabled.
    """
    return spark_test_session.spark


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
def external_dataproducts_created(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory, testrun_uid: str
) -> None:
    """Create external dataproducts (databases, tables and views) as needed by tests."""
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
