import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

from opengeh_silver.infrastructure.config.table_names import TableNames
from opengeh_silver.infrastructure.settings.database_settings import DatabaseSettings
from tests.schemas.silver_measurements_schema import silver_measurements_schema


def test__migrations__should_create_silver_measurements_table(spark: SparkSession, migrate):
    # Arrange
    database_settings = DatabaseSettings()  # type: ignore

    # Assert
    silver_measurements = spark.table(f"{database_settings.silver_database_name}.{TableNames.silver_measurements}")
    assert_schemas.assert_schema(actual=silver_measurements.schema, expected=silver_measurements_schema)
