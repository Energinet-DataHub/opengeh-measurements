from pyspark.sql import DataFrame

import core.silver.application.config.spark_session as spark_session
import core.silver.domain.transformations.measurements_transformation as measurements_transformation
import core.silver.infrastructure.protobuf.persist_submitted_transaction as persist_submitted_transaction
from core.bronze.infrastructure.repositories.invalid_submitted_transactions_repository import (
    InvalidSubmittedTransactionsRepository,
)
from core.bronze.infrastructure.streams.bronze_repository import BronzeRepository
from core.silver.infrastructure.streams.silver_repository import SilverRepository


def stream_submitted_transactions() -> None:
    spark = spark_session.initialize_spark()
    submitted_transactions = BronzeRepository(spark).read_submitted_transactions()
    SilverRepository().write_stream(submitted_transactions, _batch_operation)


def _batch_operation(submitted_transactions: DataFrame, batchId: int) -> None:
    (valid_submitted_transactions, invalid_submitted_transactions) = persist_submitted_transaction.unpack(
        submitted_transactions
    )

    _handle_valid_submitted_transactions(valid_submitted_transactions)
    _handle_invalid_submitted_transactions(invalid_submitted_transactions)


def _handle_valid_submitted_transactions(submitted_transactions: DataFrame) -> None:
    spark = spark_session.initialize_spark()
    measurements = measurements_transformation.create_by_unpacked_submitted_transactions(spark, submitted_transactions)
    SilverRepository().append(measurements)


def _handle_invalid_submitted_transactions(invalid_submitted_transactions: DataFrame) -> None:
    InvalidSubmittedTransactionsRepository().append(invalid_submitted_transactions)
