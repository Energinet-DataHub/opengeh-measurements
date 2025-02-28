import pytest
from geh_common.pyspark.read_csv import read_csv_path
from geh_common.testing.delta_lake.delta_lake_operations import create_database, create_table
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain import calculated_measurements_schema
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.electrical_heating.infrastructure import (
    MeasurementsGoldDatabaseDefinition,
    electrical_heating_v1,
)


@pytest.fixture(scope="session")
def test_files_folder_path(tests_path: str) -> str:
    return f"{tests_path}/job_tests/test_files"


@pytest.fixture(scope="session")
def seed_gold_table(spark: SparkSession, test_files_folder_path: str) -> None:
    create_database(spark, MeasurementsGoldDatabaseDefinition.DATABASE_NAME)

    create_table(
        spark,
        database_name=MeasurementsGoldDatabaseDefinition.DATABASE_NAME,
        table_name=MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME,
        schema=electrical_heating_v1,
        table_location=f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}/{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}",
    )

    file_name = f"{test_files_folder_path}/{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}-{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}.csv"
    time_series_points = read_csv_path(spark, file_name, electrical_heating_v1)
    time_series_points.write.saveAsTable(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}",
        format="delta",
        mode="overwrite",
    )


@pytest.fixture(scope="session")
def create_calculated_measurements_table(spark: SparkSession, test_files_folder_path: str) -> None:
    create_table(
        spark,
        database_name=CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME,
        table_name=CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_NAME,
        schema=calculated_measurements_schema,
        table_location=f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}/{CalculatedMeasurementsInternalDatabaseDefinition.TIME_SERIES_POINTS_NAME}",
    )


@pytest.fixture(scope="session")
def job_environment_variables(test_files_folder_path) -> dict:
    return {
        "CATALOG_NAME": "spark_catalog",
        "TIME_ZONE": "Europe/Copenhagen",
        "ELECTRICITY_MARKET_DATA_PATH": test_files_folder_path,
    }
