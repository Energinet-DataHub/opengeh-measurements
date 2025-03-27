import geh_common.testing.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

import core.silver.infrastructure.protobuf.persist_submitted_transaction as sut
import tests.helpers.binary_helper as binary_helper
from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import (
    ValueColumnNames,
)
from tests.helpers.builders.submitted_transactions_builder import SubmittedTransactionsBuilder
from tests.silver.schemas.bronze_submitted_transactions_value_schema import bronze_submitted_transactions_value_schema


def test__unpack__should_return_expected_schema(spark: SparkSession) -> None:
    # Arrange
    submitted_transactions = SubmittedTransactionsBuilder(spark).add_row().build()

    # Act
    (actual_valid, actual_invalid) = sut.unpack(submitted_transactions)

    # Assert
    assert_schemas.assert_schema(
        actual_valid.schema, bronze_submitted_transactions_value_schema, ignore_nullability=True
    )
    actual_value = actual_valid.collect()[0]
    assert actual_value[ValueColumnNames.version] is not None
    assert actual_value[ValueColumnNames.orchestration_instance_id] is not None
    assert actual_value[ValueColumnNames.orchestration_type] is not None
    assert actual_value[ValueColumnNames.metering_point_id] is not None
    assert actual_value[ValueColumnNames.transaction_id] is not None
    assert actual_value[ValueColumnNames.transaction_creation_datetime] is not None
    assert actual_value[ValueColumnNames.metering_point_type] is not None
    assert actual_value[ValueColumnNames.unit] is not None
    assert actual_value[ValueColumnNames.resolution] is not None
    assert actual_value[ValueColumnNames.start_datetime] is not None
    assert actual_value[ValueColumnNames.end_datetime] is not None
    assert actual_value[ValueColumnNames.points] is not None
    actual_points = actual_value[ValueColumnNames.points]
    assert len(actual_points) == 1
    assert actual_points[0].position is not None
    assert actual_points[0].quantity is not None
    assert actual_points[0].quality is not None


def test__create_by_submitted_transactions__when_message_does_not_match_protobuf__should_filter_away(
    spark: SparkSession,
) -> None:
    # Arrange
    value = binary_helper.generate_random_binary()
    submitted_transactions = SubmittedTransactionsBuilder(spark).add_row(value=value).build()

    # Act
    (actual_valid, actual_invalid) = sut.unpack(submitted_transactions)

    # Assert
    assert actual_valid.count() == 0
    assert actual_invalid.count() == 1
