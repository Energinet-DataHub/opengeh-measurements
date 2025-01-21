import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

from silver.domain.schemas.silver_measurements import (
    silver_measurements_schema,
)
from silver.infrastructure.silver.database_names import DatabaseNames
from silver.infrastructure.silver.table_names import TableNames


def test__migrations__should_create_silver_measurements_table(spark: SparkSession, migrate):
    # Assert
    silver_measurements = spark.table(f"{DatabaseNames.silver_database}.{TableNames.silver_measurements_table}")
    assert_schemas.assert_schema(actual=silver_measurements.schema, expected=silver_measurements_schema)
