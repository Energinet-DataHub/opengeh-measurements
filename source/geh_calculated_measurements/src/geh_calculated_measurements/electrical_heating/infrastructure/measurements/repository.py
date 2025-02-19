from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.electrical_heating.infrastructure.measurements.calculated_measurements.wrapper import (
    CalculatedMeasurements,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.measurements_gold.database_definitions import (
    MeasurementsGoldDatabase,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.measurements_gold.wrapper import (
    TimeSeriesPoints,
)


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
        schema_name: str,
        time_series_points_table: str,
    ) -> None:
        self._spark = spark
        self._calculated_measurements_full_table = f"{catalog_name}.{schema_name}.{time_series_points_table}"

    def write_calculated_measurements(
        self, calculated_measurements: CalculatedMeasurements, write_mode: str = "append"
    ) -> None:
        calculated_measurements.df.write.format("delta").mode(write_mode).saveAsTable(
            self._calculated_measurements_full_table
        )

    def read_calculated_measurements(self) -> CalculatedMeasurements:
        return CalculatedMeasurements(self._spark.read.table(self._calculated_measurements_full_table))

    def read_time_series_points(self) -> TimeSeriesPoints:
        return TimeSeriesPoints(self._spark.read.table(self._calculated_measurements_full_table))

    def _read_view_or_table(
        self,
        table_name: str,
    ) -> DataFrame:
        name = f"{self._catalog_name}.{MeasurementsGoldDatabase.DATABASE_NAME}.{table_name}"
        return self._spark.read.format("delta").table(name)
