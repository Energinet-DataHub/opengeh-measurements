from typing import Callable

from pyspark.sql import DataFrame

import core.gold.infrastructure.config.spark as spark_session
import core.utility.delta_table_helper as delta_table_helper
from core.gold.domain.constants.column_names.gold_measurements_column_names import GoldMeasurementsColumnNames
from core.gold.infrastructure.config import GoldTableNames
from core.settings import StorageAccountSettings
from core.settings.gold_settings import GoldSettings
from core.settings.streaming_settings import StreamingSettings
from core.utility import shared_helpers


class GoldMeasurementsRepository:
    def __init__(self) -> None:
        self.gold_container_name = GoldSettings().gold_container_name
        self.gold_database_name = GoldSettings().gold_database_name
        self.data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT
        self.table = f"{self.gold_database_name}.{GoldTableNames.gold_measurements}"
        self.spark = spark_session.initialize_spark()

    def write_stream(
        self,
        source_table: DataFrame,
        batch_operation: Callable[["DataFrame", int], None],
    ) -> bool | None:
        query_name = "measurements_silver_to_gold"
        checkpoint_location = shared_helpers.get_checkpoint_path(
            self.data_lake_settings, self.gold_container_name, GoldTableNames.gold_measurements
        )

        write_stream = (
            source_table.writeStream.format("delta")
            .queryName(query_name)
            .option("checkpointLocation", checkpoint_location)
        )

        if StreamingSettings().continuous_streaming_enabled is False:
            write_stream = write_stream.trigger(availableNow=True)

        return write_stream.foreachBatch(batch_operation).start().awaitTermination()

    def append_if_not_exists(self, gold_measurements: DataFrame) -> None:
        """Append to the table unless there are duplicates based on all columns except 'created'.

        :param gold_measurements: DataFrame containing the data to be appended.
        """
        delta_table_helper.append_if_not_exists(
            self.spark,
            gold_measurements,
            self.table,
            self._merge_columns(),
        )

    def _merge_columns(self) -> list[str]:
        return [
            GoldMeasurementsColumnNames.metering_point_id,
            GoldMeasurementsColumnNames.orchestration_type,
            GoldMeasurementsColumnNames.observation_time,
            GoldMeasurementsColumnNames.quantity,
            GoldMeasurementsColumnNames.quality,
            GoldMeasurementsColumnNames.metering_point_type,
            GoldMeasurementsColumnNames.transaction_id,
            GoldMeasurementsColumnNames.transaction_creation_datetime,
        ]
