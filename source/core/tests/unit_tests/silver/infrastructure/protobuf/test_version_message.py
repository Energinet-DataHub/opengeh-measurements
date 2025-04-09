from pyspark.sql import SparkSession

import core.silver.infrastructure.protobuf.version_message as version_message
import tests.helpers.protobuf_helper as protobuf_helper
from tests.helpers.builders.submitted_transactions_builder import SubmittedTransactionsBuilder, ValueBuilder


def test__with_version__adds_version_column_to_dataframe(spark: SparkSession) -> None:
    # Arrange
    version = "1"
    value = ValueBuilder(spark).add_row(version=version).build()
    protobuf_message = SubmittedTransactionsBuilder(spark).add_row(value=value).build()

    # Act
    actual = version_message.with_version(protobuf_message)

    # Assert
    actual_row = actual.collect()[0]
    assert actual_row["version"] == str(version)


def test__with_version__when_protobuf_message_cannot_be_parsed__should_return_invalid_dataframe(
    spark: SparkSession,
) -> None:
    # Arrange
    value = protobuf_helper.generate_random_binary()
    protobuf_message = SubmittedTransactionsBuilder(spark).add_row(value=value).build()

    # Act
    actual = version_message.with_version(protobuf_message)

    # Assert
    actual_row = actual.collect()[0]
    assert actual_row["version"] is None
