from pyspark.sql import DataFrame

import core.silver.domain.transformations.persist_submitted_transaction_transformationV2 as persist_submitted_transaction_transformationV2
import core.silver.infrastructure.protobuf.persist_submitted_transactionV2 as persist_submitted_transactionV2
from core.contracts.process_manager.PersistSubmittedTransaction.persist_submitted_transaction_proto_version import (
    PersistSubmittedTransactionProtoVersion,
)
from core.silver.domain.protobuf.protobuf import ProtoDeserializerBase


class PersistSubmittedTransactionV2(ProtoDeserializerBase):
    @property
    def version(self) -> str:
        return PersistSubmittedTransactionProtoVersion.version_2

    def transform(self, submitted_transactions: DataFrame) -> DataFrame:
        return persist_submitted_transaction_transformationV2.transform(submitted_transactions)

    def unpack(self, submitted_transactions: DataFrame) -> tuple[DataFrame, DataFrame]:
        return persist_submitted_transactionV2.unpack(submitted_transactions)
