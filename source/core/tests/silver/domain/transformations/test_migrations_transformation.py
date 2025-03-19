from decimal import Decimal

import geh_common.testing.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

import core.silver.domain.transformations.migrations_transformation as mit
from tests.helpers.builders.migrated_transactions_builder import MigratedTransactionsBuilder
from tests.silver.schemas.silver_measurements_schema import silver_measurements_schema


def test__transform__should_return_expected_schema(spark: SparkSession) -> None:
    # Arrange
    migrated_transactions = MigratedTransactionsBuilder(spark).add_row().build()

    # Act
    actual = mit.transform(spark, migrated_transactions)

    # Assert
    assert_schemas.assert_schema(actual.schema, silver_measurements_schema, ignore_nullability=True)


def test__transform__should_return_correct_decimal_value(spark: SparkSession) -> None:
    # Arrange
    expected_decimal_value = Decimal(1.5)
    migrated_transactions = (
        MigratedTransactionsBuilder(spark).add_row(values=[(0, "D01", expected_decimal_value)]).build()
    )

    # Act
    actual = mit.transform(spark, migrated_transactions)

    # Assert
    assert actual.collect()[0].points[0].quantity == expected_decimal_value
