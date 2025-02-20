from decimal import Decimal

import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

import core.silver.domain.transformations.measurements_transformation as sut
from tests.helpers.builders.submitted_transactions_value_builder import (
    DecimalValue,
    Point,
    SubmittedTransactionsValueBuilder,
)
from tests.silver.schemas.silver_measurements_schema import silver_measurements_schema


def test__create_by_submitted_transactions__should_return_expected_schema(spark: SparkSession) -> None:
    # Arrange
    submitted_transactions = SubmittedTransactionsValueBuilder(spark).add_row().build()

    # Act
    actual = sut.create_by_unpacked_submitted_transactions(spark, submitted_transactions)

    # Assert
    assert_schemas.assert_schema(actual.schema, silver_measurements_schema, ignore_nullability=True)


def test__create_by_submitted_transaction__should_return_correct_decimal_value(spark: SparkSession) -> None:
    # Arrange
    expected_decimal_value = Decimal(1.5)
    point = Point(quantity=DecimalValue(units=1, nanos=500000))
    submitted_transaction = SubmittedTransactionsValueBuilder(spark).add_row(points=[point]).build()

    # Act
    actual = sut.create_by_unpacked_submitted_transactions(spark, submitted_transaction)

    # Assert
    assert actual.collect()[0].points[0].quantity == expected_decimal_value
