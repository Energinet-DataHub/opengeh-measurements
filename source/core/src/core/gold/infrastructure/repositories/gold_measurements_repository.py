from pyspark.sql import DataFrame

import core.gold.infrastructure.config.spark as spark_session
import core.utility.delta_table_helper as delta_table_helper
from core.gold.domain.constants.column_names.gold_measurements_column_names import GoldMeasurementsColumnNames
from core.gold.infrastructure.config import GoldTableNames
from core.settings.gold_settings import GoldSettings


class GoldMeasurementsRepository:
    def __init__(self) -> None:
        self.gold_database_name = GoldSettings().gold_database_name
        self.table = f"{self.gold_database_name}.{GoldTableNames.gold_measurements}"
        self.spark = spark_session.initialize_spark()

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
