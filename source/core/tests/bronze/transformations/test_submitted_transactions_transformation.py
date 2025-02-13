import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

import core.bronze.domain.transformations.submitted_transactions_transformation as sut
from tests.helpers.builders.submitted_transactions_builder import SubmittedTransactionsBuilder
from tests.silver.schemas.bronze_submitted_transactions_value_schema import bronze_submitted_transactions_value_schema


def test__create_by_submitted_transactions__should_return_expected_schema(spark: SparkSession) -> None:
    # Arrange
    submitted_transactions = SubmittedTransactionsBuilder(spark).add_row().build()

    # Act
    actual = sut.created_by_packed_submitted_transactions(submitted_transactions)

    # Assert
    assert_schemas.assert_schema(actual.schema, bronze_submitted_transactions_value_schema, ignore_nullability=True)
