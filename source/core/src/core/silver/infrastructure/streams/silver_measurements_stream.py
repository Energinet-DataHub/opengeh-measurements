from typing import Callable

from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from pyspark.sql import DataFrame

import core.utility.shared_helpers as shared_helpers
from core.settings import StorageAccountSettings
from core.settings.silver_settings import SilverSettings
from core.settings.streaming_settings import StreamingSettings


class SilverMeasurementsStream:
    def __init__(self) -> None:
        self.data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT
        self.silver_container_name = SilverSettings().silver_container_name

    def write_stream(
        self,
        measurements: DataFrame,
        orchestration_type: GehCommonOrchestrationType,
        batch_operation: Callable[["DataFrame", int], None],
    ) -> bool | None:
        checkpoint_location = self._get_streaming_checkpoint_path(orchestration_type)

        write_stream = (
            measurements.writeStream.outputMode("append")
            .format("delta")
            .option("checkpointLocation", checkpoint_location)
        )

        stream_settings = StreamingSettings()
        write_stream = stream_settings.apply_streaming_settings(write_stream)

        return write_stream.foreachBatch(batch_operation).start().awaitTermination()

    def _get_streaming_checkpoint_path(self, orchestration_type: GehCommonOrchestrationType) -> str:
        if orchestration_type == GehCommonOrchestrationType.SUBMITTED:
            checkpoint_name = "submitted_transactions"
        elif orchestration_type == GehCommonOrchestrationType.MIGRATION:
            checkpoint_name = "migrated_transactions"

        return shared_helpers.get_checkpoint_path(self.data_lake_settings, self.silver_container_name, checkpoint_name)
