from typing import Callable

from pyspark.sql import DataFrame

import core.utility.shared_helpers as shared_helpers
from core.generics.stream import Stream
from core.settings import StorageAccountSettings
from core.settings.silver_settings import SilverSettings


class SilverMeasurementsStream(Stream):
    def __init__(self) -> None:
        self.data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT
        self.silver_container_name = SilverSettings().silver_container_name

    def stream_submitted_transactions(
        self, measurements: DataFrame, batch_operation: Callable[["DataFrame", int], None]
    ) -> bool | None:
        query_name = "stream_bronze_submitted_transactions_to_silver"
        checkpoint_name = "submitted_transactions"
        checkpoint_location = shared_helpers.get_checkpoint_path(
            self.data_lake_settings, self.silver_container_name, checkpoint_name
        )

        return self.write_stream(measurements, checkpoint_location, query_name, batch_operation)

    def stream_migrated_transactions(
        self, measurements: DataFrame, batch_operation: Callable[["DataFrame", int], None]
    ) -> bool | None:
        query_name = "stream_bronze_migrated_transactions_to_silver"
        checkpoint_name = "migrated_transactions"
        checkpoint_location = shared_helpers.get_checkpoint_path(
            self.data_lake_settings, self.silver_container_name, checkpoint_name
        )

        return self.write_stream(measurements, checkpoint_location, query_name, batch_operation)
