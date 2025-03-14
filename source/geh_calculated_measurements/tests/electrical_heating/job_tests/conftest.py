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
from tests.electrical_heating.job_tests import get_test_files_folder_path


@pytest.fixture(scope="session")
def gold_table_seeded(spark: SparkSession) -> None:
    create_database(spark, MeasurementsGoldDatabaseDefinition.DATABASE_NAME)

    create_table(
        spark,
        database_name=MeasurementsGoldDatabaseDefinition.DATABASE_NAME,
        table_name=MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME,
        schema=electrical_heating_v1,
        table_location=f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}/{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}",
    )

    file_name = f"{get_test_files_folder_path()}/{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}-{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}.csv"
    time_series_points = read_csv_path(spark, file_name, electrical_heating_v1)
    time_series_points.write.option("overwriteSchema", "true").saveAsTable(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}",
        format="delta",
        mode="overwrite",
    )


@pytest.fixture(scope="session")
def calculated_measurements_table_created(spark: SparkSession) -> None:
    create_database(spark, CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME)

    create_table(
        spark,
        database_name=CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME,
        table_name=CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_NAME,
        schema=calculated_measurements_schema,
        table_location=f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}/{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_NAME}",
    )
