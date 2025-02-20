from typing import Callable

from pyspark.sql import DataFrame

import core.utility.shared_helpers as shared_helpers
from core.bronze.infrastructure.settings import (
    StorageAccountSettings,
    SubmittedTransactionsStreamSettings,
)
from core.settings.silver_settings import SilverSettings
from core.silver.infrastructure.config import SilverTableNames


class SilverRepository:
    def __init__(self) -> None:
        database_name = SilverSettings().silver_database_name  # type: ignore
        self.table = f"{database_name}.{SilverTableNames.silver_measurements}"
        self.data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT  # type: ignore
        self.silver_container_name = SilverSettings().silver_container_name  # type: ignore

    def write_stream(
        self,
        measurements: DataFrame,
        batch_operation: Callable[["DataFrame", int], None],
    ) -> bool | None:
        checkpoint_location = shared_helpers.get_checkpoint_path(
            self.data_lake_settings, self.silver_container_name, "submitted_transactions"
        )

        write_stream = (
            measurements.writeStream.outputMode("append")
            .format("delta")
            .option("checkpointLocation", checkpoint_location)
        )

        stream_settings = SubmittedTransactionsStreamSettings()  # type: ignore

        if stream_settings.continuous_streaming_enabled is False:
            write_stream = write_stream.trigger(availableNow=True)

        return write_stream.foreachBatch(batch_operation).start().awaitTermination()

    def append(self, measurements: DataFrame) -> None:
        measurements.write.format("delta").mode("append").saveAsTable(self.table)
