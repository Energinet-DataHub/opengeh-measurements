import pyspark.sql.functions as F
from pyspark.sql import DataFrame

import core.silver.application.protobuf.protobuf_versions as protobuf_versions
from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import (
    ValueColumnNames,
)
from core.bronze.infrastructure.repositories.invalid_submitted_transactions_repository import (
    InvalidSubmittedTransactionsRepository,
)


def handle(submitted_transactions: DataFrame) -> None:
    versions = protobuf_versions.get_versions()
    unknown_protobuf_messages = submitted_transactions.filter(~F.col(ValueColumnNames.version).isin(versions))
    InvalidSubmittedTransactionsRepository().append(unknown_protobuf_messages)
