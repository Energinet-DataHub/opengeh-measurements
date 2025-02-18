import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

import core.bronze.domain.transformations.transactions_persisted_events_transformation as sut
from core.bronze.domain.events.transactions_persisted_event import (
    transactions_persisted_event,
)
from tests.bronze.helpers.builders.submitted_transactions_builder import (
    SubmittedTransactionsBuilder,
)


def test__transform__given_protobuf_message__when_called__then_return_dataframe_with_expected_schema(
    spark: SparkSession,
):
    # Arrange
    submitted_transactions = SubmittedTransactionsBuilder(spark).add_row().build()

    # Act
    actual = sut.transform(submitted_transactions)

    # Assert
    assert_schemas.assert_schema(actual.schema, transactions_persisted_event, ignore_nullability=True)
