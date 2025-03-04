import datetime
from decimal import Decimal

import pytest
from geh_common.testing.delta_lake.delta_lake_operations import create_database, create_table
from pyspark.sql import SparkSession

from geh_calculated_measurements.electrical_heating.infrastructure import (
    MeasurementsGoldDatabaseDefinition,
    MeasurementsGoldRepository,
    electrical_heating_v1,
)


@pytest.fixture(scope="session")
def setup_test_table(spark: SparkSession) -> None:
    """Create a test database and table for measurements_gold."""
    # Create the database
    create_database(spark, MeasurementsGoldDatabaseDefinition.DATABASE_NAME)

    # Create the table with the appropriate schema
    create_table(
        spark,
        database_name=MeasurementsGoldDatabaseDefinition.DATABASE_NAME,
        table_name=MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME,
        schema=electrical_heating_v1,
        table_location=f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}/{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}",
    )
    test_data = spark.createDataFrame(
        [
            ("123456789012345", "consumption", datetime.datetime(2022, 1, 1, 0, 0, 0), Decimal("10.500")),
            ("123456789012345", "consumption", datetime.datetime(2022, 1, 1, 1, 0, 0), Decimal("12.750")),
            ("223456789012345", "electrical_heating", datetime.datetime(2022, 1, 1, 0, 0, 0), Decimal("25.000")),
            ("323456789012345", "supply_to_grid", datetime.datetime(2022, 1, 1, 0, 0, 0), Decimal("5.250")),
            ("423456789012345", "consumption_from_grid", datetime.datetime(2022, 1, 2, 0, 0, 0), Decimal("8.125")),
            ("523456789012345", "net_consumption", datetime.datetime(2022, 1, 2, 0, 0, 0), Decimal("15.000")),
        ],
        schema=electrical_heating_v1,
    )

    test_data.write.format("delta").mode("overwrite").saveAsTable(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}_base"
    )


@pytest.fixture(scope="session")
def get_default_catalog(spark: SparkSession) -> str:
    return spark.catalog.currentCatalog()


@pytest.fixture(scope="session")
def measurements_gold_repository(
    spark: SparkSession, setup_test_table: None, get_default_catalog: str
) -> MeasurementsGoldRepository:
    return MeasurementsGoldRepository(
        spark,
        get_default_catalog,
    )


@pytest.fixture(autouse=True)
def reset_test_table_after_each_test(spark: SparkSession, setup_test_table) -> None:
    """Reset the test table to its original state after each test."""
    # The test runs here
    yield

    # After the test completes, restore the original table
    original_table = f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}_base"
    test_table = f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}"

    # Copy from base table back to test table
    df_original = spark.read.table(original_table)
    df_original.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(test_table)
