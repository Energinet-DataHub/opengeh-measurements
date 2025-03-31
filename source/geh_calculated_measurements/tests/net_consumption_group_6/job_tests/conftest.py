import pytest
from geh_common.pyspark.read_csv import read_csv_path
from geh_common.testing.delta_lake.delta_lake_operations import create_database, create_table
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain import CalculatedMeasurements, CurrentMeasurements
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from geh_calculated_measurements.net_consumption_group_6.infrastucture.database_definitions import (
    ElectricityMarketMeasurementsInputDatabaseDefinition,
)
from geh_calculated_measurements.net_consumption_group_6.infrastucture.schema import (
    net_consumption_group_6_child_metering_point_v1,
    net_consumption_group_6_consumption_metering_point_periods_v1,
)
from tests.net_consumption_group_6.job_tests import get_test_files_folder_path


@pytest.fixture(autouse=True)
def gold_table_seeded(spark: SparkSession) -> None:
    create_database(spark, MeasurementsGoldDatabaseDefinition.DATABASE_NAME)

    create_table(
        spark,
        database_name=MeasurementsGoldDatabaseDefinition.DATABASE_NAME,
        table_name=MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS,
        schema=CurrentMeasurements.schema,
        table_location=f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}/{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}",
    )

    file_name = f"{get_test_files_folder_path()}/{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}-{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}.csv"
    time_series_points = read_csv_path(spark, file_name, CurrentMeasurements.schema)
    time_series_points.write.option("overwriteSchema", "true").saveAsTable(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}",
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


@pytest.fixture(autouse=True)
def electricity_market_calculated_measurements_create_and_seed_tables(spark: SparkSession) -> None:
    create_database(spark, ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME)

    # Create and seed parent/child tables
    parent_file_name = f"{get_test_files_folder_path()}/consumption_metering_point_periods_v1.csv"
    consumption_metering_point_periods = read_csv_path(
        spark, parent_file_name, net_consumption_group_6_consumption_metering_point_periods_v1
    )
    consumption_metering_point_periods.write.option("overwriteSchema", "true").saveAsTable(
        f"{ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CONSUMPTION_METERING_POINT_PERIODS}",
        format="delta",
        mode="overwrite",
    )

    child_file_name = f"{get_test_files_folder_path()}/child_metering_points_v1.csv"
    child_metering_points = read_csv_path(spark, child_file_name, net_consumption_group_6_child_metering_point_v1)
    child_metering_points.write.option("overwriteSchema", "true").saveAsTable(
        f"{ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINT}",
        format="delta",
        mode="overwrite",
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
