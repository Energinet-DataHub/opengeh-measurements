import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

from opengeh_bronze.domain.schemas.migrated import migrated_schema
from opengeh_bronze.infrastructure.migrated_transactions_repository import (
    MigratedTransactionsRepository,
)


def test__read_measurements_bronze_migrated__should_return_the_correct_dataframe(spark: SparkSession, migrate):
    # Arrange
    repo = MigratedTransactionsRepository(spark)

    # Act
    migrated_transactions = repo.read_measurements_bronze_migrated()

    # Assert
    assert_schemas.assert_schema(actual=migrated_transactions.schema, expected=migrated_schema)


def test__calculate_latest_created_timestamp_that_has_been_migrated__should_return_none_when_empty(
    spark: SparkSession, migrate: None
):
    # Arrange
    repo = MigratedTransactionsRepository(spark)
    test_succeeded = True

    # Act
    migrated_transactions = repo.calculate_latest_created_timestamp_that_has_been_migrated()

    # Assert
    assert migrated_transactions is None
