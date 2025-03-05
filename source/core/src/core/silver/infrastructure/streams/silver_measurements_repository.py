from typing import Callable

from pyspark.sql import DataFrame

import core.utility.shared_helpers as shared_helpers
from core.bronze.infrastructure.settings import (
    SubmittedTransactionsStreamSettings,
)
from core.settings import StorageAccountSettings
from core.settings.silver_settings import SilverSettings
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames
from core.silver.infrastructure.config import SilverTableNames


class SilverMeasurementsRepository:
    def __init__(self) -> None:
        database_name = SilverSettings().silver_database_name
        self.table = f"{database_name}.{SilverTableNames.silver_measurements}"
        self.data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT
        self.silver_container_name = SilverSettings().silver_container_name

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

        stream_settings = SubmittedTransactionsStreamSettings()

        if stream_settings.continuous_streaming_enabled is False:
            write_stream = write_stream.trigger(availableNow=True)

        return write_stream.foreachBatch(batch_operation).start().awaitTermination()

    def append(self, measurements: DataFrame) -> None:
        measurements.write.format("delta").mode("append").saveAsTable(self.table)

    def append_if_not_exists(self, silver_measurements: DataFrame) -> None:
        """Append to the table unless there are duplicates based on all columns except 'created'.

        :param silver_measurements: DataFrame containing the data to be appended.
        """
        existing_data = silver_measurements.sparkSession.table(self.table)
        new_data = silver_measurements.alias("new_data")

        condition = [new_data[column] == existing_data[column] for column in self._merge_columns()]

        filtered_data = new_data.join(existing_data.alias("existing_data"), condition, "left_anti")

        filtered_data.write.format("delta").mode("append").saveAsTable(self.table)

    def _merge_columns(self) -> list[str]:
        return [
            SilverMeasurementsColumnNames.orchestration_type,
            SilverMeasurementsColumnNames.orchestration_instance_id,
            SilverMeasurementsColumnNames.metering_point_id,
            SilverMeasurementsColumnNames.transaction_id,
            SilverMeasurementsColumnNames.transaction_creation_datetime,
            SilverMeasurementsColumnNames.metering_point_type,
            SilverMeasurementsColumnNames.unit,
            SilverMeasurementsColumnNames.resolution,
            SilverMeasurementsColumnNames.start_datetime,
            SilverMeasurementsColumnNames.end_datetime,
            SilverMeasurementsColumnNames.points,
            SilverMeasurementsColumnNames.is_cancelled,
            SilverMeasurementsColumnNames.is_deleted,
        ]
