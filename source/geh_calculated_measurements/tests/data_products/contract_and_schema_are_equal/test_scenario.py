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
    view_name = "hourly_calculated_measurements_v1"
    schema = CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME
    catalog = CatalogSettings().catalog_name
    view_df = spark.sql(f"SELECT * FROM {catalog}.{schema}.{view_name}")

    contract_schema = hourly_calculated_measurements_v1.hourly_calculated_measurements_v1

    assert_contract(actual_schema=view_df.schema, contract=contract_schema)
