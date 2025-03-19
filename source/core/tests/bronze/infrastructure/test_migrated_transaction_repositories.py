from datetime import datetime, timedelta
from unittest.mock import patch

import geh_common.testing.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

from core.bronze.domain.schemas.migrated_transactions import migrated_transactions_schema
from core.bronze.infrastructure.repositories.migrated_transactions_repository import (
    MigratedTransactionsRepository,
)
from tests.helpers.builders.migrated_transactions_builder import MigratedTransactionsBuilder


def test__read__should_return_the_correct_dataframe(spark: SparkSession, migrations_executed: None) -> None:
    # Arrange
    repo = MigratedTransactionsRepository(spark)

    # Act
    migrated_transactions = repo.read()

    # Assert
    assert_schemas.assert_schema(actual=migrated_transactions.schema, expected=migrated_transactions_schema)


def test__calculate_latest_created_timestamp_that_has_been_migrated__should_return_none_when_empty(
    spark: SparkSession, migrations_executed: None
) -> None:
    # Arrange
    repo = MigratedTransactionsRepository(spark)

    migrated_data = MigratedTransactionsBuilder(spark)
    migrated_data = migrated_data.build()

    with patch.object(repo, "read", return_value=migrated_data):
        # Act
        migrated_transactions = repo.calculate_latest_created_timestamp_that_has_been_migrated()

        # Assert
        assert migrated_transactions is None


def test__calculate_latest_created_timestamp_that_has_been_migrated__should_return_correct_datetime(
    spark: SparkSession, migrations_executed: None
) -> None:
    # Arrange
    repo = MigratedTransactionsRepository(spark)

    timestamp_to_find = datetime.now()

    NUM_TRANSFERED = 1000
    migrated_data = MigratedTransactionsBuilder(spark)
    for i in range(NUM_TRANSFERED):
        migrated_data.add_row(created_in_migrations=datetime.now() - timedelta(days=30))
    migrated_data.add_row(created_in_migrations=timestamp_to_find)  # Add the one we care about.
    migrated_data = migrated_data.build()

    with patch.object(repo, "read", return_value=migrated_data.orderBy(rand())):
        # Act
        result_max_created = repo.calculate_latest_created_timestamp_that_has_been_migrated()

        # Assert
        assert result_max_created == timestamp_to_find
