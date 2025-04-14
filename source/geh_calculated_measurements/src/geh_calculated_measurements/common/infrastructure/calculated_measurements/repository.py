from pyspark.sql import SparkSession

from geh_calculated_measurements.common.application.model import CalculatedMeasurementsInternal
from geh_calculated_measurements.common.domain.model.current_measurements import CurrentMeasurements
from geh_calculated_measurements.common.infrastructure.calculated_measurements.database_definitions import (
    CalculatedMeasurementsInternalDatabaseDefinition,
)
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str | None = None,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def _get_full_table_path(self, database_name: str, table_name: str) -> str:
        if self._catalog_name:
            return f"{self._catalog_name}.{database_name}.{table_name}"
        return f"{database_name}.{table_name}"

    def write_calculated_measurements(self, data: CalculatedMeasurementsInternal) -> None:
        df = data.df
        database_name = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
        table_name = CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME
        df.write.format("delta").mode("append").saveAsTable(self._get_full_table_path(database_name, table_name))

    def read_current_measurements(self) -> CurrentMeasurements:
        database_name = MeasurementsGoldDatabaseDefinition.DATABASE_NAME
        table_name = MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS
        df = self._spark.table(self._get_full_table_path(database_name, table_name))
        return CurrentMeasurements(df)
