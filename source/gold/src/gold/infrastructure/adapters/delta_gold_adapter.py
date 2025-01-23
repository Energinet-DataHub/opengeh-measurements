from typing import Callable

from pyspark.sql import DataFrame

from gold.application.ports.gold_port import GoldPort
from gold.infrastructure.config.database_names import DatabaseNames
from gold.infrastructure.shared_helpers import (
    EnvironmentVariable,
    get_checkpoint_path,
    get_env_variable_or_throw,
    get_full_table_name,
)


class DeltaGoldAdapter(GoldPort):
    def start_write_stream(self, df_source_stream: DataFrame, query_name: str, table_name: str, batch_operation: Callable[["DataFrame", int], None]) -> None:
        datalake_storage_account = get_env_variable_or_throw(EnvironmentVariable.DATALAKE_STORAGE_ACCOUNT)
        checkpoint_location = get_checkpoint_path(datalake_storage_account, DatabaseNames.gold_database, table_name)
        (df_source_stream.writeStream
         .format("delta")
         .queryName(query_name)
         .option("checkpointLocation", checkpoint_location)
         .foreachBatch(batch_operation)
         .trigger(availableNow=True)
         .start()
         .awaitTermination())

    def append(self, df: DataFrame, table_name: str) -> None:
        df.write.format("delta").mode("append").saveAsTable(get_full_table_name(DatabaseNames.gold_database, table_name))
