from pyspark.sql import DataFrame

import core.silver.domain.transformations.persist_submitted_transaction_transformation as persist_submitted_transaction_transformation
import core.silver.infrastructure.protobuf.persist_submitted_transaction as persist_submitted_transaction
from core.contracts.process_manager.PersistSubmittedTransaction.persist_submitted_transaction_proto_version import (
    PersistSubmittedTransactionProtoVersion,
)
from core.silver.domain.protobuf.protobuf import ProtoDeserializerBase


class PersistSubmittedTransactionV1(ProtoDeserializerBase):
    @property
    def version(self) -> str:
        return PersistSubmittedTransactionProtoVersion.version_1

    def transform(self, submitted_transactions: DataFrame) -> DataFrame:
        return persist_submitted_transaction_transformation.transform(submitted_transactions)

    def unpack(self, submitted_transactions: DataFrame) -> tuple[DataFrame, DataFrame]:
        return persist_submitted_transaction.unpack(submitted_transactions)
