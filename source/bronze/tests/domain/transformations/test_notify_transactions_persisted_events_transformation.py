from pyspark.sql import SparkSession

import opengeh_bronze.domain.transformations.notify_transactions_persisted_events_transformation as sut
from tests.helpers.builders.submitted_transactions_builder import SubmittedTransactionsBuilder


def test__transform__given_protobuf_message__when_called__then_return_dataframe_with_mapped_fields(spark: SparkSession):
    # Arrange
    submitted_transactions = SubmittedTransactionsBuilder(spark).add_row().build()

    # Act
    actual = sut.transform(submitted_transactions)

    # Assert
    assert actual.count() == 1
