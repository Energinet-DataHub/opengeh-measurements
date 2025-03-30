import geh_common.testing.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

from core.bronze.domain.schemas.submitted_transactions_quarantined import submitted_transactions_quarantined_schema
from core.bronze.domain.transformations.submitted_transactions_quarantined_transformations import (
    map_silver_measurements_to_submitted_transactions_quarantined,
)
from tests.helpers.builders.submitted_transactions_quarantined_builder import SubmittedTransactionsQuarantinedBuilder


def test__map_silver_measurements_to_submitted_transactions_quarantined__should_map(
    spark: SparkSession,
) -> None:
    # Arrange
    submitted_transactions_quarantined = SubmittedTransactionsQuarantinedBuilder(spark).add_row().build()

    # Act
    actual = map_silver_measurements_to_submitted_transactions_quarantined(submitted_transactions_quarantined)

    # Assert
    result = actual.collect()
    assert len(result) == 1
    assert_schemas.assert_schema(actual=actual.schema, expected=submitted_transactions_quarantined_schema)
