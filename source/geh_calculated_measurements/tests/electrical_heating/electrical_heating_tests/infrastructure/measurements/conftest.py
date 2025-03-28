import datetime
from decimal import Decimal

import pytest
from geh_common.testing.delta_lake.delta_lake_operations import create_database, create_table
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.common.infrastructure import CurrentMeasurementsRepository
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)


@pytest.fixture(autouse=True)
def measurements_gold_with_data(spark: SparkSession) -> None:
    """Create a test database and table for measurements_gold."""
    # Create the database
    create_database(spark, MeasurementsGoldDatabaseDefinition.DATABASE_NAME)

    # Create the table with the appropriate schema
    create_table(
        spark,
        database_name=MeasurementsGoldDatabaseDefinition.DATABASE_NAME,
        table_name=MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS,
        schema=CurrentMeasurements.schema,
        table_location=f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}/{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}",
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
        schema=CurrentMeasurements.schema,
    )

    test_data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}"
    )


@pytest.fixture(scope="session")
def default_catalog(spark: SparkSession) -> str:
    return spark.catalog.currentCatalog()


@pytest.fixture(scope="session")
def measurements_gold_repository(spark: SparkSession, default_catalog: str) -> CurrentMeasurementsRepository:
    return CurrentMeasurementsRepository(
        spark,
        default_catalog,
    )
