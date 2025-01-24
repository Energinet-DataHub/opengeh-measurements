import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

from bronze.application.migrations import TableNames
from bronze.infrastructure.schemas.submitted_transactions import (
    submitted_transactions_schema,
)
from tests.conftest import DatabaseNames


def test__ingest_submitted_transactions__should_create_submitted_transactions_table(
    spark: SparkSession, submitted_transactions, submit_transactions: None
):
    # Assert
    submitted_transactions = spark.table(f"{DatabaseNames.bronze_database}.{TableNames.submitted_transactions_table}")
    assert_schemas.assert_schema(actual=submitted_transactions.schema, expected=submitted_transactions_schema)
