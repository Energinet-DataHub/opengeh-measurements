﻿from pyspark.sql import SparkSession

import tests.helpers.assert_helper as assert_helper
from database_migration.constants.database_constants import DatabaseConstants
from database_migration.constants.table_constants import TableConstants
from database_migration.schemas.bronze_measurements import (
    calculation_results_bronze_schema,
)


def test__migrations__should_create_bronze_measurements_table(spark: SparkSession, migrate):
    # Assert
    bronze_measurements = spark.table(f"{DatabaseConstants.bronze_database}.{TableConstants.bronze_measurements_table}")
    assert_helper.assert_schemas(bronze_measurements.schema, calculation_results_bronze_schema)