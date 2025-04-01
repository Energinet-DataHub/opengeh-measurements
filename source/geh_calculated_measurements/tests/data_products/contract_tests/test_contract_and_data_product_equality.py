import pytest
from geh_common.testing.dataframes import assert_contract
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsDatabaseDefinition
from geh_calculated_measurements.contracts.data_products import (
    hourly_calculated_measurements_v1,
    missing_measurements_log_v1,
)
from geh_calculated_measurements.database_migrations.settings.catalog_settings import CatalogSettings


@pytest.mark.parametrize(
    ("view_name", "contract_schema"),
    [
        (
            CalculatedMeasurementsDatabaseDefinition.HOURLY_CALCULATED_MEASUREMENTS_VIEW_NAME,
            hourly_calculated_measurements_v1.hourly_calculated_measurements_v1,
        ),
        (
            CalculatedMeasurementsDatabaseDefinition.MISSING_MEASUREMENTS_LOG_VIEW_NAME,
            missing_measurements_log_v1.missing_measurements_log_v1,
        ),
    ],
)
def test_contract_and_schema_are_equal_parametrized(
    migrations_executed: None,  # Used implicitly
    spark: SparkSession,
    view_name: str,
    contract_schema,
) -> None:
    # Arrange
    database = CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME
    catalog = CatalogSettings().catalog_name

    # Act
    view_df = spark.table(f"{catalog}.{database}.{view_name}").limit(1)

    # Assert
    assert_contract(actual_schema=view_df.schema, contract=contract_schema)
