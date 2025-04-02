from pyspark.sql import DataFrame

import core.silver.infrastructure.config.spark_session as spark_session
from core.receipts.infrastructure.config.table_names import CoreInternalTableNames
from core.settings.core_internal_settings import CoreInternalSettings


class ReceiptsRepository:
    def __init__(self) -> None:
        database_name = CoreInternalSettings().core_internal_database_name
        table_name = CoreInternalTableNames.process_manager_receipts
        self.table = f"{database_name}.{table_name}"
        self.spark = spark_session.initialize_spark()

    def read(self) -> DataFrame:
        return (
            self.spark.readStream.format("delta")
            .option("ignoreDeletes", "true")
            .option("skipChangeCommits", "true")
            .table(self.table)
        )
