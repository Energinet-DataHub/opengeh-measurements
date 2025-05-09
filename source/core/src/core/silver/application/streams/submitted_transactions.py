from pyspark.sql import DataFrame

import core.bronze.application.submitted_transactions.unknown_submitted_transactions_handler as unknown_submitted_transactions_handler
import core.silver.application.measurements.measurements_handler as measurements_handler
import core.silver.application.protobuf.protobuf_versions as protobuf_versions
import core.silver.infrastructure.config.spark_session as spark_session
import core.silver.infrastructure.protobuf.version_message as version_message
from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import ValueColumnNames
from core.bronze.infrastructure.repositories.invalid_submitted_transactions_repository import (
    InvalidSubmittedTransactionsRepository,
)
from core.bronze.infrastructure.repositories.submitted_transactions_repository import SubmittedTransactionsRepository
from core.silver.infrastructure.streams.silver_measurements_stream import SilverMeasurementsStream


def stream_submitted_transactions() -> None:
    spark = spark_session.initialize_spark()
    submitted_transactions = SubmittedTransactionsRepository(spark).read()
    SilverMeasurementsStream().stream_submitted_transactions(
        submitted_transactions,
        _batch_operation,
    )


def _batch_operation(submitted_transactions: DataFrame, batchId: int) -> None:
    submitted_transactions = version_message.with_version(submitted_transactions)

    for protobuf_message in protobuf_versions.protobuf_messages:
        protobuf_message = protobuf_message()
        transactions = submitted_transactions.filter(f"{ValueColumnNames.version} = '{protobuf_message.version}'")
        (valid_submitted_transactions, invalid_submitted_transactions) = protobuf_message.unpack(transactions)
        measurements_handler.handle(valid_submitted_transactions, protobuf_message)
        InvalidSubmittedTransactionsRepository().append(invalid_submitted_transactions)

    unknown_submitted_transactions_handler.handle(submitted_transactions)
