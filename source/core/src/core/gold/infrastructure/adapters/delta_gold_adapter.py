from typing import Callable

from pyspark.sql import DataFrame

from core.gold.application.ports.gold_port import GoldPort
from core.settings.catalog_settings import CatalogSettings
from core.utility.environment_variable_helper import EnvironmentVariable, get_env_variable_or_throw
from core.utility.shared_helpers import get_checkpoint_path, get_full_table_name


class DeltaGoldAdapter(GoldPort):
    def start_write_stream(
        self,
        df_source_stream: DataFrame,
        query_name: str,
        table_name: str,
        batch_operation: Callable[["DataFrame", int], None],
        terminate_on_empty: bool = False,
    ) -> None:
        catalog_settings = CatalogSettings()  # type: ignore
        datalake_storage_account = get_env_variable_or_throw(EnvironmentVariable.DATALAKE_STORAGE_ACCOUNT)
        checkpoint_location = get_checkpoint_path(
            datalake_storage_account, catalog_settings.gold_container_name, table_name
        )
        df_write_stream = (
            df_source_stream.writeStream.format("delta")
            .queryName(query_name)
            .option("checkpointLocation", checkpoint_location)
            .foreachBatch(batch_operation)
        )

        if terminate_on_empty:
            df_write_stream.trigger(availableNow=True).start().awaitTermination()
        else:
            df_write_stream.start().awaitTermination()

    def append(self, df: DataFrame, table_name: str) -> None:
        catalog_settings = CatalogSettings()  # type: ignore
        df.write.format("delta").mode("append").saveAsTable(
            get_full_table_name(catalog_settings.gold_database_name, table_name)
        )
