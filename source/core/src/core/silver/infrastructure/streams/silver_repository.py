from pyspark.sql import DataFrame

from core.bronze.infrastructure.settings import (
    StorageAccountSettings,
    SubmittedTransactionsStreamSettings,
)
from core.settings.catalog_settings import CatalogSettings
from core.silver.infrastructure.config import SilverTableNames
from core.utility.shared_helpers import get_checkpoint_path


class SilverRepository:
    def __init__(self) -> None:
        database_name = CatalogSettings().silver_database_name  # type: ignore
        self.table = f"{database_name}.{SilverTableNames.silver_measurements}"
        self.data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT  # type: ignore
        self.silver_container_name = CatalogSettings().silver_container_name  # type: ignore

    def write_measurements(self, measurements: DataFrame) -> None:
        checkpoint_location = get_checkpoint_path(
            self.data_lake_settings, self.silver_container_name, "submitted_transactions"
        )

        write_stream = measurements.writeStream.outputMode("append").option("checkpointLocation", checkpoint_location)

        stream_settings = SubmittedTransactionsStreamSettings()  # type: ignore

        if stream_settings.continuous_streaming_enabled is False:
            write_stream = write_stream.trigger(availableNow=True)

        write_stream.toTable(self.table)
