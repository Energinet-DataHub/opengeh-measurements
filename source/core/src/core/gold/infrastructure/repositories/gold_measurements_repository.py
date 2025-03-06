from typing import Callable

from pyspark.sql import DataFrame

import core.utility.shared_helpers as shared_helpers
from core.gold.domain.constants.column_names.gold_measurements_column_names import GoldMeasurementsColumnNames
from core.gold.infrastructure.config import GoldTableNames
from core.settings.gold_settings import GoldSettings
from core.utility.environment_variable_helper import EnvironmentVariable, get_env_variable_or_throw


class GoldMeasurementsRepository:
    def __init__(self) -> None:
        self.gold_container_name = GoldSettings().gold_container_name
        self.gold_database_name = GoldSettings().gold_database_name
        self.table = f"{self.gold_database_name}.{GoldTableNames.gold_measurements}"

    def start_write_stream(
        self,
        df_source_stream: DataFrame,
        batch_operation: Callable[["DataFrame", int], None],
        terminate_on_empty: bool = False,
    ) -> None:
        query_name = "measurements_silver_to_gold"
        datalake_storage_account = get_env_variable_or_throw(EnvironmentVariable.DATALAKE_STORAGE_ACCOUNT)
        checkpoint_location = shared_helpers.get_checkpoint_path(
            datalake_storage_account, self.gold_container_name, GoldTableNames.gold_measurements
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

    def append_if_not_exists(self, gold_measurements: DataFrame) -> None:
        """Append to the table unless there are duplicates based on all columns except 'created'.

        :param gold_measurements: DataFrame containing the data to be appended.
        """
        existing_data = gold_measurements.sparkSession.table(self.table)
        new_data = gold_measurements.alias("new_data")

        condition = [new_data[column] == existing_data[column] for column in self._merge_columns()]

        filtered_data = new_data.join(existing_data.alias("existing_data"), condition, "left_anti")

        filtered_data.write.format("delta").mode("append").saveAsTable(self.table)

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
