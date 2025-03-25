from pyspark.sql import DataFrame

from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import (
    ValueColumnNames,
)
from core.bronze.infrastructure.repositories.invalid_submitted_transactions_repository import (
    InvalidSubmittedTransactionsRepository,
)
from core.silver.application.protobuf.protobuf_versions import ProtobufVersions


def handle(submitted_transactions: DataFrame) -> None:
    collected_versions = ",".join(ProtobufVersions().get_versions())
    unknown_protobuf_messages = submitted_transactions.filter(
        f"{ValueColumnNames.version} not in ({collected_versions})"
    )
    InvalidSubmittedTransactionsRepository().append(unknown_protobuf_messages)
