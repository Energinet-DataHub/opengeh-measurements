from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from pyspark.sql import DataFrame

import core.bronze.application.submitted_transactions.submitted_transactions_handler as submitted_transactions_handler
import core.silver.infrastructure.config.spark_session as spark_session
import core.silver.infrastructure.protobuf.version_message as version_message
from core.bronze.infrastructure.streams.bronze_repository import BronzeRepository
from core.silver.application.versions.protobuf_versions import ProtobufVersions
from core.silver.infrastructure.repositories.silver_measurements_repository import SilverMeasurementsRepository


def stream_submitted_transactions() -> None:
    spark = spark_session.initialize_spark()
    submitted_transactions = BronzeRepository(spark).read_submitted_transactions()
    SilverMeasurementsRepository().write_stream(
        submitted_transactions,
        GehCommonOrchestrationType.SUBMITTED,
        _batch_operation,
    )


def _batch_operation(submitted_transactions: DataFrame, batchId: int) -> None:
    submitted_transactions = version_message.with_version(submitted_transactions)

    for protobuf_message in ProtobufVersions().protobuf_messages:
        protobuf_message = protobuf_message()
        transactions = submitted_transactions.filter(f"version = {protobuf_message.version}")
        (valid_submitted_transactions, invalid_submitted_transactions) = protobuf_message.unpack(transactions)
        protobuf_message.handle_valid_protobuf(valid_submitted_transactions)
        submitted_transactions_handler.persist_invalid_submitted_transactions(invalid_submitted_transactions)

    submitted_transactions_handler.handle_unknown_submitted_transaction(submitted_transactions)
