from pyspark.sql import DataFrame

import core.silver.infrastructure.config.spark_session as spark_session
from core.gold.domain.constants.column_names.gold_measurements_series_sap_column_names import (
    GoldMeasurementsSeriesSAPColumnNames,
)
from core.gold.infrastructure.config import GoldTableNames
from core.settings.gold_settings import GoldSettings
from core.utility import delta_table_helper


class GoldMeasurementsSeriesSAPRepository:
    def __init__(self) -> None:
        database_name = GoldSettings().gold_database_name
        self.table = f"{database_name}.{GoldTableNames.gold_measurements_series_sap}"
        self.spark = spark_session.initialize_spark()

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

    def _merge_columns(self) -> list[str]:
        return [
            GoldMeasurementsSeriesSAPColumnNames.orchestration_type,
            GoldMeasurementsSeriesSAPColumnNames.metering_point_id,
            GoldMeasurementsSeriesSAPColumnNames.transaction_id,
            GoldMeasurementsSeriesSAPColumnNames.transaction_creation_datetime,
            GoldMeasurementsSeriesSAPColumnNames.start_time,
            GoldMeasurementsSeriesSAPColumnNames.resolution,
        ]
