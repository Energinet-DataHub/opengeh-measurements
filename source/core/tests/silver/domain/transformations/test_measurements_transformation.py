import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

import core.silver.domain.transformations.measurements_transformation as sut
from tests.helpers.builders.submitted_transactions_builder import SubmittedTransactionsBuilder
from tests.silver.schemas.silver_measurements_schema import silver_measurements_schema


def test__create_by_submitted_transactions__should_return_expected_schema(spark: SparkSession) -> None:
    # Arrange
    submitted_transactions = SubmittedTransactionsBuilder(spark).add_row().build()

    # Act
    actual = sut.create_by_unpacked_submitted_transactions(spark, submitted_transactions)

    # Assert
    assert_schemas.assert_schema(actual.schema, silver_measurements_schema, ignore_nullability=True)
