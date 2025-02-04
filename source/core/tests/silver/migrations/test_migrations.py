import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

from silver.schemas.silver_measurements_schema import silver_measurements_schema
from src.core.silver.infrastructure.config.database_names import DatabaseNames
from src.core.silver.infrastructure.config.table_names import TableNames


def test__migrations__should_create_silver_measurements_table(spark: SparkSession, migrate):
    # Assert
    silver_measurements = spark.table(f"{DatabaseNames.silver}.{TableNames.silver_measurements}")
    assert_schemas.assert_schema(actual=silver_measurements.schema, expected=silver_measurements_schema)
