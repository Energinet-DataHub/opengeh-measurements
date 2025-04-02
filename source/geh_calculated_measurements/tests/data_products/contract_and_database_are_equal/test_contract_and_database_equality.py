from geh_common.testing.dataframes import assert_contract
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsDatabaseDefinition
from geh_calculated_measurements.contracts.data_products import hourly_calculated_measurements_v1
from tests import SPARK_CATALOG_NAME


def test_contract_and_schema_are_equal(
    migrations_executed: None,  # Used implicitly
    spark: SparkSession,
) -> None:
    # Arrange
    view_name = CalculatedMeasurementsDatabaseDefinition.HOURLY_CALCULATED_MEASUREMENTS_VIEW_NAME
    database = CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME
    contract_schema = hourly_calculated_measurements_v1.hourly_calculated_measurements_v1

    # Act
    view_df = spark.table(f"{SPARK_CATALOG_NAME}.{database}.{view_name}").limit(1)

    # Assert
    assert_contract(actual_schema=view_df.schema, contract=contract_schema)
