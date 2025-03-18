from pyspark.sql import DataFrame

from core.bronze.infrastructure.config import BronzeTableNames
from core.settings.bronze_settings import BronzeSettings


class SubmittedTransactionsQuarantinedRepository:
    def __init__(self) -> None:
        database_name = BronzeSettings().bronze_database_name
        self.table = f"{database_name}.{BronzeTableNames.bronze_submitted_transactions_quarantined}"

    def append(self, submitted_transactions_quarantined: DataFrame) -> None:
        submitted_transactions_quarantined.write.format("delta").mode("append").saveAsTable(self.table)
