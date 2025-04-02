from typing import Callable

from pyspark.sql import DataFrame

from core.settings import StorageAccountSettings
from core.settings.gold_settings import GoldSettings
from core.settings.streaming_settings import StreamingSettings
from core.utility import shared_helpers


class GoldMeasurementsStream:
    def __init__(self) -> None:
        self.gold_container_name = GoldSettings().gold_container_name
        self.data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT

    def write_stream(
        self,
        checkpoint_name: str,
        query_name: str,
        source_table: DataFrame,
        batch_operation: Callable[["DataFrame", int], None],
    ) -> bool | None:
        query_name = "measurements_silver_to_gold"
        checkpoint_location = shared_helpers.get_checkpoint_path(
            self.data_lake_settings, self.gold_container_name, checkpoint_name
        )

        write_stream = (
            source_table.writeStream.format("delta")
            .queryName(query_name)
            .option("checkpointLocation", checkpoint_location)
        )

        write_stream = StreamingSettings().apply_streaming_settings(write_stream)

        return write_stream.foreachBatch(batch_operation).start().awaitTermination()
