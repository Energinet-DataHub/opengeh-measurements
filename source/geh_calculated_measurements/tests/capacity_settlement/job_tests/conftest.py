import pytest
from geh_common.pyspark.read_csv import read_csv_path
from geh_common.testing.delta_lake.delta_lake_operations import create_database, create_table
from pyspark.sql import SparkSession

from geh_calculated_measurements.capacity_settlement.infrastructure import (
    MeasurementsGoldDatabaseDefinition,
)
from geh_calculated_measurements.capacity_settlement.infrastructure.measurements_gold.schema import (
    capacity_settlement_v1,
)
from geh_calculated_measurements.common.domain import calculated_measurements_schema
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from tests.capacity_settlement.job_tests import TEST_FILES_FOLDER_PATH


@pytest.fixture(scope="session")
def gold_table_seeded(spark: SparkSession) -> None:
    create_database(spark, MeasurementsGoldDatabaseDefinition.DATABASE_NAME)

    create_table(
        spark,
        database_name=MeasurementsGoldDatabaseDefinition.DATABASE_NAME,
        table_name=MeasurementsGoldDatabaseDefinition.MEASUREMENTS,
        schema=capacity_settlement_v1,
        table_location=f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}/{MeasurementsGoldDatabaseDefinition.MEASUREMENTS}",
    )

    file_name = f"{TEST_FILES_FOLDER_PATH}/{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}-{MeasurementsGoldDatabaseDefinition.MEASUREMENTS}.csv"
    time_series_points = read_csv_path(spark, file_name, capacity_settlement_v1)
    time_series_points.write.saveAsTable(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.MEASUREMENTS}",
        format="delta",
        mode="overwrite",
    )


@pytest.fixture(scope="session")
def calculated_measurements_table_created(spark: SparkSession) -> None:
    create_database(spark, CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME)

    create_table(
        spark,
        database_name=CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME,
        table_name=CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME,
        schema=calculated_measurements_schema,
        table_location=f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}/{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}",
    )
