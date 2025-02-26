from pyspark.sql import DataFrame

from core.bronze.infrastructure.config import BronzeTableNames
from core.settings.bronze_settings import BronzeSettings


class InvalidSubmittedTransactionsRepository:
    def __init__(self) -> None:
        database_name = BronzeSettings().bronze_database_name  # type: ignore
        self.table = f"{database_name}.{BronzeTableNames.bronze_invalid_submitted_transactions}"

    def append(self, invalid_submitted_transactions: DataFrame) -> None:
        invalid_submitted_transactions.write.format("delta").mode("append").saveAsTable(self.table)
