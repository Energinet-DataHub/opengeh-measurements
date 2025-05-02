import os
import sys
from datetime import datetime
from decimal import Decimal
from typing import Generator
from unittest import mock

import geh_common.telemetry.logging_configuration
import pytest
from geh_common.domain.types import MeteringPointType, QuantityQuality
from geh_common.telemetry.logging_configuration import configure_logging
from geh_common.testing.dataframes import AssertDataframesConfiguration, configure_testing
from geh_common.testing.delta_lake import create_database
from geh_common.testing.delta_lake.delta_lake_operations import create_table
from geh_common.testing.spark.spark_test_session import get_spark_test_session
from pyspark.sql import SparkSession

from geh_calculated_measurements.database_migrations import DatabaseNames
from geh_calculated_measurements.database_migrations.migrations_runner import _migrate
from tests import (
    SPARK_CATALOG_NAME,
    TESTS_ROOT,
    create_job_environment_variables,
)
from tests.external_data_products import ExternalDataProducts
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.testsession_configuration import TestSessionConfiguration


def pytest_sessionfinish(session, exitstatus):
    """This hook is called after the test session has finished.

    If no tests are found, pytest will exit with status code 5. This causes the
    CI/CD pipeline to fail. This hook will set the exit status to 0 if no tests are found.
    This is useful for the CI/CD pipeline to not fail if no tests are found.

    This is a long debated feature of pytest, and unfortunately it has been decided
    to keep it as is. See discussions here:
    - https://github.com/pytest-dev/pytest/issues/2393
    - https://github.com/pytest-dev/pytest/issues/812
    - https://github.com/pytest-dev/pytest/issues/500
    """
    if exitstatus == 5:
        session.exitstatus = 0


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
    # shutil.rmtree(data_dir)


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
        DatabaseNames.MEASUREMENTS_CALCULATED,
        DatabaseNames.MEASUREMENTS_CALCULATED_INTERNAL,
    ]:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    _migrate(SPARK_CATALOG_NAME)


@pytest.fixture(scope="session")
def external_dataproducts_created(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory, testrun_uid: str
) -> None:
    """Create external data products (databases, tables and views) as needed by tests."""

    for database_name in ExternalDataProducts.get_all_database_names():
        create_database(spark, database_name)

    for dataproduct in ExternalDataProducts.get_all_data_products():
        create_table(
            spark,
            database_name=dataproduct.database_name,
            table_name=dataproduct.view_name,
            schema=dataproduct.schema,
        )


def seed_current_measurements(
    spark: SparkSession,
    metering_point_id: str,
    observation_time: datetime,
    quantity: Decimal = Decimal("1.0"),
    metering_point_type: MeteringPointType = MeteringPointType.CONSUMPTION,
    quantity_quality: QuantityQuality = QuantityQuality.MEASURED,
) -> None:
    database_name = ExternalDataProducts.CURRENT_MEASUREMENTS.database_name
    table_name = ExternalDataProducts.CURRENT_MEASUREMENTS.view_name
    schema = ExternalDataProducts.CURRENT_MEASUREMENTS.schema

    measurements = spark.createDataFrame(
        [
            (
                metering_point_id,
                observation_time,
                quantity,
                quantity_quality.value,
                metering_point_type.value,
            )
        ],
        schema=schema,
    )

    measurements.write.saveAsTable(
        f"{database_name}.{table_name}",
        format="delta",
        mode="append",
    )


@pytest.fixture(scope="session")
def test_table_configuration(
    spark: SparkSession,
    migrations_executed: TestSessionConfiguration,
) -> None:
    """Ensures all tables is configured correctly. tests that all tables have:
    - liquad clustering
    - 30 days retention period
    - is managed
    - delta format
    """
    errors = []

    # List all catalogs
    catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]

    for catalog in catalogs:
        # List all schemas in the catalog
        schemas = [row.namespace for row in spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()]

        for schema in schemas:
            # List all tables in the schema
            tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()

            for table in tables:
                table_full_name = f"{catalog}.{schema}.{table.tableName}"

                # Describe table details
                try:
                    desc = spark.sql(f"DESCRIBE TABLE EXTENDED {table_full_name}").collect()
                except Exception as e:
                    errors.append(f"Failed to describe table {table_full_name}: {e}")
                    continue

                # Extract key properties
                properties = {
                    row.col_name.strip(): row.data_type.strip() for row in desc if row.col_name and row.data_type
                }

                # Check format
                if "Provider" not in properties or properties["Provider"].lower() != "delta":
                    errors.append(f"{table_full_name} is not in Delta format.")

                # Check managed table
                if "Table Properties" in properties:
                    if "'EXTERNAL'" in properties["Table Properties"]:
                        errors.append(f"{table_full_name} is not a managed table.")
                elif "Type" in properties and properties["Type"].lower() != "managed":
                    errors.append(f"{table_full_name} is not a managed table.")

                # Check retention period
                retention = properties.get("Retention", "")
                if retention != "30":
                    errors.append(f"{table_full_name} does not have 30-day retention (found: {retention}).")

                # Check liquid clustering
                tbl_props_rows = [row for row in desc if row.col_name == "Table Properties"]
                if tbl_props_rows:
                    tbl_props_str = tbl_props_rows[0].data_type
                    if "delta.feature.liquidClustering.enabled=true" not in tbl_props_str:
                        errors.append(f" {table_full_name} does not have liquid clustering enabled.")
                else:
                    errors.append(f" {table_full_name} has no table properties set.")

    if errors:
        error_message = "\n".join(errors)
        raise AssertionError(f"\n🔍 Table configuration issues found:\n{error_message}")
