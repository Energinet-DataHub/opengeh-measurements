from pyspark.sql import SparkSession

import tests.helpers.assert_helper as assert_helper
from bronze.domain.constants.database_names import DatabaseNames
from bronze.domain.constants.table_names import TableNames
from bronze.infrastructure.schemas.bronze_measurements import (
    calculation_results_bronze_schema,
)


def test__migrations__should_create_bronze_measurements_table(spark: SparkSession, migrate):
    # Assert
    bronze_measurements = spark.table(f"{DatabaseNames.bronze_database}.{TableNames.bronze_measurements_table}")
    assert_helper.assert_schemas(bronze_measurements.schema, calculation_results_bronze_schema)
