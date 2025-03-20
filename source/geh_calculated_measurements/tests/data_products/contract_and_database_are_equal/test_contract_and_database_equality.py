from geh_common.testing.dataframes import AssertDataframesConfiguration, assert_contract
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsDatabaseDefinition
from geh_calculated_measurements.contracts.data_products import hourly_calculated_measurements_v1
from geh_calculated_measurements.database_migrations.settings.catalog_settings import CatalogSettings


def test_contract_and_schema_are_equal(
    migrations_executed: None,
    patch_environment: None,
    assert_dataframes_configuration: AssertDataframesConfiguration,
    spark: SparkSession,
) -> None:
    # Arrange
    view_name = CalculatedMeasurementsDatabaseDefinition.HOURLY_CALCULATED_MEASUREMENTS_VIEW_NAME
    database = CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME
    catalog = CatalogSettings().catalog_name
    contract_schema = hourly_calculated_measurements_v1.hourly_calculated_measurements_v1

    # Act
    view_df = spark.table(f"{catalog}.{database}.{view_name}").limit(1)

    # Assert
    assert_contract(actual_schema=view_df.schema, contract=contract_schema)
