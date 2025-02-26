### This file contains the fixtures that are used in the tests. ###
from pathlib import Path
from typing import Generator

import pytest
from geh_common.pyspark.read_csv import read_csv_path
from geh_common.telemetry.logging_configuration import configure_logging
from geh_common.testing.delta_lake.delta_lake_operations import create_database, create_table
from pyspark.sql import SparkSession

from geh_calculated_measurements.electrical_heating.domain import time_series_points_v1
from geh_calculated_measurements.electrical_heating.infrastructure import ElectricityMarketRepository
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.measurements_gold.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from tests import PROJECT_ROOT
from tests.electrical_heating.testsession_configuration import TestSessionConfiguration


@pytest.fixture(scope="module", autouse=True)
def clear_cache(spark: SparkSession) -> Generator[None, None, None]:
    """
    Clear the cache after each test module to avoid memory issues.
    """
    yield
    spark.catalog.clearCache()


@pytest.fixture(autouse=True)
def configure_dummy_logging() -> None:
    """Ensure that logging hooks don't fail due to _TRACER_NAME not being set."""

    configure_logging(cloud_role_name="any-cloud-role-name", tracer_name="any-tracer-name")


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


@pytest.fixture(scope="session")
def electricity_market_repository_extra_col(
    spark: SparkSession, test_files_folder_path: str
) -> ElectricityMarketRepository:
    return ElectricityMarketRepository(
        spark,
        test_files_folder_path,
        consumption_metering_point_periods_file_name="consumption_metering_point_periods_v1_extra_col.csv",
        child_metering_points_file_name="child_metering_points_v1_extra_col.csv",
    )


@pytest.fixture(scope="session")
def electricity_market_repository_missing_col(
    spark: SparkSession, test_files_folder_path: str
) -> ElectricityMarketRepository:
    return ElectricityMarketRepository(
        spark,
        test_files_folder_path,
        consumption_metering_point_periods_file_name="consumption_metering_point_periods_v1_missing_col.csv",
        child_metering_points_file_name="child_metering_points_v1_missing_col.csv",
    )


@pytest.fixture(scope="session")
def electricity_market_repository(spark: SparkSession, test_files_folder_path: str) -> ElectricityMarketRepository:
    return ElectricityMarketRepository(spark, test_files_folder_path)
