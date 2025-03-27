import pytest
from geh_common.pyspark.read_csv import read_csv_path
from geh_common.testing.delta_lake.delta_lake_operations import create_database, create_table
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain import CalculatedMeasurements
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.net_consumption_group_6.infrastucture import (
    MeasurementsGoldDatabaseDefinition,
    electrical_heating_v1,
)
from tests.net_consumption_group_6.job_tests import get_test_files_folder_path


@pytest.fixture(autouse=True)
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


@pytest.fixture(autouse=True)
def calculated_measurements_table_created(spark: SparkSession) -> None:
    create_database(spark, CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME)

    create_table(
        spark,
        database_name=CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME,
        table_name=CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME,
        schema=CalculatedMeasurements.schema,
        table_location=f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}/{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}",
    )


def test_calculated_measurements_table_creation(
    spark: SparkSession, calculated_measurements_table_created: None
) -> None:
    """
    Test that the calculated_measurements_table_created fixture creates the expected table.
    """
    # Check if table exists
    tables = spark.sql(f"SHOW TABLES IN {CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}").collect()
    table_names = [t.tableName for t in tables]
    assert CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME in table_names
