from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

import core.bronze.application.submitted_transactions.submitted_transactions_handler as submitted_transactions_handler
import core.silver.infrastructure.protobuf.persist_submitted_transaction as persist_submitted_transaction


class Protobuf(ABC):
    @property
    @abstractmethod
    def version(self) -> str:
        pass

    @abstractmethod
    def transform(self) -> None:
        pass

    @abstractmethod
    def unpack(self) -> tuple[DataFrame, DataFrame]:
        pass


class PersistSubmittedTransaction(Protobuf):
    def version(self) -> str:
        return "1"

    def transform(self, submitted_transactions: DataFrame) -> None:
        submitted_transactions_handler.handle_valid_submitted_transactions(submitted_transactions)

    def unpack(self, submitted_transactions: DataFrame) -> tuple[DataFrame, DataFrame]:
        return persist_submitted_transaction.unpack(submitted_transactions)
