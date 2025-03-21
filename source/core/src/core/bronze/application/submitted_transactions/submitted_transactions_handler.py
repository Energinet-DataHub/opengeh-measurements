from pyspark.sql import DataFrame

from core.bronze.infrastructure.repositories.invalid_submitted_transactions_repository import (
    InvalidSubmittedTransactionsRepository,
)
from core.silver.application.protobuf.protobuf_versions import ProtobufVersions


def handle_unknown_submitted_transaction(submitted_transactions: DataFrame) -> None:
    collected_versions = ProtobufVersions().get_versions()
    unknown_protobuf_messages = submitted_transactions.filter(f"version not in ({','.join(collected_versions)})")
    persist_invalid_submitted_transactions(unknown_protobuf_messages)


def persist_invalid_submitted_transactions(invalid_submitted_transactions: DataFrame) -> None:
    InvalidSubmittedTransactionsRepository().append(invalid_submitted_transactions)
