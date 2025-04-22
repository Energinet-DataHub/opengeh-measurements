from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from pyspark.sql import DataFrame

import core.silver.infrastructure.config.spark_session as spark_session
from core.settings.silver_settings import SilverSettings
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames
from core.silver.infrastructure.config import SilverTableNames
from core.utility import delta_table_helper


class SilverMeasurementsRepository:
    def __init__(self) -> None:
        database_name = SilverSettings().silver_database_name
        self.table = f"{database_name}.{SilverTableNames.silver_measurements}"
        self.spark = spark_session.initialize_spark()

    def read(self) -> DataFrame:
        return (
            self.spark.readStream.format("delta")
            .option("ignoreDeletes", "true")
            .option("skipChangeCommits", "true")
            .table(self.table)
        )

    def read_submitted(self) -> DataFrame:
        return (
            self.spark.readStream.format("delta")
            .option("ignoreDeletes", "true")
            .option("skipChangeCommits", "true")
            .table(self.table)
            .filter(
                f"{SilverMeasurementsColumnNames.orchestration_type} = '{GehCommonOrchestrationType.SUBMITTED.value}'"
            )
        )

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
