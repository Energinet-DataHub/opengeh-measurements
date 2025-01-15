import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

from silver.domain.constants.database_names import DatabaseNames
from silver.domain.constants.table_names import TableNames
from silver.infrastructure.schemas.silver_measurements import (
    calculation_results_silver_schema,
)


def test__migrations__should_create_silver_measurements_table(spark: SparkSession, migrate):
    # Assert
    silver_measurements = spark.table(f"{DatabaseNames.silver_database}.{TableNames.silver_measurements_table}")
    assert_schemas.assert_schema(actual=silver_measurements.schema, expected=calculation_results_silver_schema)
