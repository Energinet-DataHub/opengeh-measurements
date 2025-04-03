from pyspark.sql import DataFrame

import core.silver.infrastructure.config.spark_session as spark_session
from core.receipts.domain.constants.column_names.process_manager_receipts_column_names import (
    ProcessManagerReceiptsColumnNames,
)
from core.receipts.infrastructure.config.table_names import CoreInternalTableNames
from core.settings.core_internal_settings import CoreInternalSettings
from core.utility import delta_table_helper


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

    def append_if_not_exists(self, df: DataFrame) -> None:
        """Append to the table unless there are duplicates based on all columns except 'created'.

        :param df: DataFrame containing the data to be appended.
        """
        spark = spark_session.initialize_spark()

        delta_table_helper.append_if_not_exists(
            spark,
            df,
            self.table,
            self._merge_columns(),
        )

    def _merge_columns(self) -> list[str]:
        return [
            ProcessManagerReceiptsColumnNames.orchestration_instance_id,
        ]
