import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

from opengeh_bronze.domain.constants.table_names import TableNames
from opengeh_bronze.domain.schemas.bronze_measurements import (
    bronze_measurements_schema,
)
from opengeh_bronze.domain.schemas.submitted_transactions import (
    submitted_transactions_schema,
)
from opengeh_bronze.infrastructure.settings import CatalogSettings

bronze_database_name = CatalogSettings().bronze_database_name  # type: ignore


def test__migrations__should_create_bronze_measurements_table(spark: SparkSession, migrate):
    # Assert
    bronze_measurements = spark.table(f"{database_name}.{TableNames.bronze_measurements_table}")
    assert_schemas.assert_schema(actual=bronze_measurements.schema, expected=bronze_measurements_schema)


def test__ingest_submitted_transactions__should_create_submitted_transactions_table(spark: SparkSession, migrate: None):
    # Assert
    submitted_transactions = spark.table(f"{database_name}.{TableNames.bronze_submitted_transactions_table}")
    assert_schemas.assert_schema(actual=submitted_transactions.schema, expected=submitted_transactions_schema)
