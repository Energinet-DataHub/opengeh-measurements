from typing import Callable

from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from pyspark.sql import DataFrame

import core.silver.infrastructure.config.spark_session as spark_session
import core.utility.shared_helpers as shared_helpers
from core.settings import StorageAccountSettings
from core.settings.silver_settings import SilverSettings
from core.settings.streaming_settings import StreamingSettings
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames
from core.silver.infrastructure.config import SilverTableNames
from core.utility import delta_table_helper


class SilverMeasurementsRepository:
    def __init__(self) -> None:
        database_name = SilverSettings().silver_database_name
        self.table = f"{database_name}.{SilverTableNames.silver_measurements}"
        self.data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT
        self.silver_container_name = SilverSettings().silver_container_name

    def read_stream(self) -> DataFrame:
        spark = spark_session.initialize_spark()
        return (
            spark.readStream.format("delta")
            .option("ignoreDeletes", "true")
            .option("skipChangeCommits", "true")
            .table(self.table)
        )

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

    def append_if_not_exists(self, silver_measurements: DataFrame) -> None:
        """Append to the table unless there are duplicates based on all columns except 'created'.

        :param silver_measurements: DataFrame containing the data to be appended.
        """
        spark = spark_session.initialize_spark()

        delta_table_helper.append_if_not_exists(
            spark,
            silver_measurements,
            self.table,
            self._merge_columns(),
        )

    def _get_streaming_checkpoint_path(self, orchestration_type: GehCommonOrchestrationType) -> str:
        if orchestration_type == GehCommonOrchestrationType.SUBMITTED:
            checkpoint_name = "submitted_transactions"
        elif orchestration_type == GehCommonOrchestrationType.MIGRATION:
            checkpoint_name = "migrated_transactions"

        return shared_helpers.get_checkpoint_path(self.data_lake_settings, self.silver_container_name, checkpoint_name)

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
        ]
