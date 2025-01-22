from typing import Callable

from pyspark.sql import DataFrame

from gold.application.ports.gold_repository import GoldRepository
from gold.entry_points import get_datalake_storage_account
from gold.infrastructure.config.database_names import DatabaseNames
from gold.infrastructure.shared_helpers import get_full_table_name, get_checkpoint_path


class DeltaGoldRepository(GoldRepository):
    def start_write_stream(self, records: DataFrame, query_name: str, table_name: str, batch_operation: Callable[["DataFrame", int], None]) -> None:
        checkpoint_location = get_checkpoint_path(get_datalake_storage_account(), DatabaseNames.gold_database, table_name)
        (records.writeStream
         .format("delta")
         .queryName(query_name)
         .option("checkpointLocation", checkpoint_location)
         .foreachBatch(batch_operation)
         .trigger(availableNow=True)
         .start()
         .awaitTermination())

    def append(self, df: DataFrame, table_name: str) -> None:
        df.write.format("delta").mode("append").saveAsTable(get_full_table_name(DatabaseNames.gold_database, table_name))
