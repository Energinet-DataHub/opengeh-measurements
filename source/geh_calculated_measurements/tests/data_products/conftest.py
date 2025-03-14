### This file contains the fixtures that are used in the tests. ###
from pathlib import Path
from typing import Generator

import pytest
from geh_common.testing.dataframes import AssertDataframesConfiguration, write_when_files_to_delta
from geh_common.testing.scenario_testing import TestCase, TestCases, get_then_names
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain.model import calculated_measurements
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsDatabaseDefinition
from geh_calculated_measurements.database_migrations.migrations_runner import migrate
from geh_calculated_measurements.database_migrations.settings.catalog_settings import CatalogSettings
from tests import PROJECT_ROOT
from tests.testsession_configuration import TestSessionConfiguration


@pytest.fixture(scope="module", autouse=True)
def clear_cache(spark: SparkSession) -> Generator[None, None, None]:
    """
    Clear the cache after each test module to avoid memory issues.
    """
    yield
    spark.catalog.clearCache()


@pytest.fixture(scope="session")
def contracts_path() -> str:
    """Returns the source/contract folder path."""
    return (PROJECT_ROOT / "src" / "geh_calculated_measurements" / "data_products" / "contracts").as_posix()


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
    # Execute all migrations
    _create_databases(spark)
    migrate()


@pytest.fixture(scope="module")
def patch_environment(env_args_fixture_logging: dict[str, str]) -> None:
    from unittest import mock

    mock.patch.dict("os.environ", env_args_fixture_logging, clear=False)


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest) -> TestCases:
    # Get the path to the scenario
    scenario_path = str(Path(request.module.__file__).parent)

    # Populate the delta tables with the 'when' files
    path_schema_tuples = [
        (
            "measurements_calculated_internal.calculated_measurements.csv",
            calculated_measurements.calculated_measurements_schema,
        )
    ]
    write_when_files_to_delta(
        spark,
        scenario_path,
        path_schema_tuples,
    )

    # Receive a list of 'then' file names
    then_files = get_then_names(scenario_path)

    # Construct a list of TestCase objects
    test_cases = []
    schema = CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME
    catalog = CatalogSettings().catalog_name
    for path_name in then_files:
        actual = spark.sql(f"SELECT * FROM {catalog}.{schema}.{path_name}")

        test_cases.append(
            TestCase(
                expected_csv_path=f"{scenario_path}/then/{path_name}.csv",
                actual=actual,
            )
        )

    # Return test cases
    return TestCases(test_cases)


@pytest.fixture(scope="session")
def assert_dataframes_configuration(
    test_session_configuration: TestSessionConfiguration,
) -> AssertDataframesConfiguration:
    return AssertDataframesConfiguration(
        show_actual_and_expected_count=test_session_configuration.scenario_tests.show_actual_and_expected_count,
        show_actual_and_expected=test_session_configuration.scenario_tests.show_actual_and_expected,
        show_columns_when_actual_and_expected_are_equal=test_session_configuration.scenario_tests.show_columns_when_actual_and_expected_are_equal,
    )
