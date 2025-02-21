### This file contains the fixtures that are used in the tests. ###
from pathlib import Path
from typing import Generator
from unittest import mock

import pytest
from geh_common.pyspark.read_csv import read_csv_path
from geh_common.telemetry.logging_configuration import LoggingSettings, configure_logging
from geh_common.testing.delta_lake.delta_lake_operations import create_database, create_table
from pyspark.sql import SparkSession

from geh_calculated_measurements.electrical_heating.infrastructure import (
    CalculatedMeasurements,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.calculated_measurements.database_definitions import (
    CalculatedMeasurementsDatabaseDefinition,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.calculated_measurements.schema import (
    calculated_measurements_schema,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.measurements_gold.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.measurements_gold.schema import (
    time_series_points_v1,
)
from tests import PROJECT_ROOT
from tests.electrical_heating.testsession_configuration import TestSessionConfiguration


@pytest.fixture(scope="session")
def env_args_fixture() -> dict[str, str]:
    env_args = {
        "CLOUD_ROLE_NAME": "test_role",
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "connection_string",
        "SUBSYSTEM": "test_subsystem",
    }
    return env_args


@pytest.fixture(scope="session")
def script_args_fixture() -> list[str]:
    sys_argv = [
        "program_name",
        "--force_configuration",
        "false",
        "--orchestration-instance-id",
        "4a540892-2c0a-46a9-9257-c4e13051d76a",
    ]
    return sys_argv


@pytest.fixture(scope="module", autouse=True)
def clear_cache(spark: SparkSession) -> Generator[None, None, None]:
    """
    Clear the cache after each test module to avoid memory issues.
    """
    yield
    spark.catalog.clearCache()


@pytest.fixture(autouse=True)
def configure_dummy_logging(env_args_fixture, script_args_fixture) -> None:
    """Ensure that logging hooks don't fail due to _TRACER_NAME not being set."""
    with (
        mock.patch("sys.argv", script_args_fixture),
        mock.patch.dict("os.environ", env_args_fixture, clear=False),
    ):
        logging_settings = LoggingSettings()
        logging_settings.applicationinsights_connection_string = None  # for testing purposes
        configure_logging(logging_settings=logging_settings, extras=None)


@pytest.fixture(scope="session")
def tests_path() -> str:
    """Returns the tests folder path."""
    return (PROJECT_ROOT / "tests" / "electrical_heating").as_posix()


@pytest.fixture(scope="session")
def contracts_path() -> str:
    """Returns the source/contract folder path."""
    return (PROJECT_ROOT / "src" / "geh_calculated_measurements" / "electrical_heating" / "contracts").as_posix()


@pytest.fixture(scope="session")
def test_session_configuration(tests_path: str) -> TestSessionConfiguration:
    settings_file_path = Path(tests_path) / "testsession.local.settings.yml"
    return TestSessionConfiguration.load(settings_file_path)


@pytest.fixture(scope="session")
def test_files_folder_path(tests_path: str) -> str:
    return f"{tests_path}/utils/test_files"


@pytest.fixture(scope="session")
def calculated_measurements(spark: SparkSession, test_files_folder_path: str) -> CalculatedMeasurements:
    create_database(spark, CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME)

    create_table(
        spark,
        database_name=CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME,
        table_name=CalculatedMeasurementsDatabaseDefinition.MEASUREMENTS_NAME,
        schema=calculated_measurements_schema,
        table_location=f"{CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME}/{CalculatedMeasurementsDatabaseDefinition.MEASUREMENTS_NAME}",
    )

    file_name = f"{test_files_folder_path}/{CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME}-{CalculatedMeasurementsDatabaseDefinition.MEASUREMENTS_NAME}.csv"

    df = read_csv_path(spark, file_name, calculated_measurements_schema)

    return CalculatedMeasurements(df)


@pytest.fixture(scope="session")
def seed_gold_table(spark: SparkSession, test_files_folder_path: str) -> None:
    create_database(spark, MeasurementsGoldDatabaseDefinition.DATABASE_NAME)

    create_table(
        spark,
        database_name=MeasurementsGoldDatabaseDefinition.DATABASE_NAME,
        table_name=MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME,
        schema=time_series_points_v1,
        table_location=f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}/{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}",
    )

    file_name = f"{test_files_folder_path}/{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}-{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}.csv"
    time_series_points = read_csv_path(spark, file_name, time_series_points_v1)
    time_series_points.write.saveAsTable(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}",
        format="delta",
        mode="overwrite",
    )
