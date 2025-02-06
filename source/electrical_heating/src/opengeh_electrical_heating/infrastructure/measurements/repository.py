from pyspark.sql import SparkSession

from opengeh_electrical_heating.infrastructure.measurements.measurements_bronze.database_definitions import (
    MeasurementsBronzeDatabase,
)
from opengeh_electrical_heating.infrastructure.measurements.measurements_bronze.wrapper import MeasurementsBronze
from opengeh_electrical_heating.infrastructure.measurements.measurements_gold.database_definitions import (
    MeasurementsGoldDatabase,
)
from opengeh_electrical_heating.infrastructure.measurements.measurements_gold.wrapper import TimeSeriesPoints


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str | None = None,
    ) -> None:
        self._spark = spark
        self._bronze_database_name = MeasurementsBronzeDatabase.DATABASE_NAME
        self._bronze_measurements_table_name = MeasurementsBronzeDatabase.MEASUREMENTS_NAME
        self._catalog_name = catalog_name
        if self._catalog_name:
            self._bronze_full_table_path = (
                f"{self._catalog_name}.{self._bronze_database_name}.{self._bronze_measurements_table_name}"
            )
        else:
            self._bronze_full_table_path = f"{self._bronze_database_name}.{self._bronze_measurements_table_name}"

    def write_measurements_bronze(self, measurements_bronze: MeasurementsBronze, write_mode: str = "append") -> None:
        measurements_bronze.df.write.format("delta").mode(write_mode).saveAsTable(self._bronze_full_table_path)

    def read_measurements_bronze(self) -> MeasurementsBronze:
        return MeasurementsBronze(self._spark.read.table(self._bronze_full_table_path))

    def read_time_series_points(self) -> TimeSeriesPoints:
        # TODO: the table does not yet exist in the database
        # df = self._read_view_or_table(
        #    MeasurementsGoldDatabase.TIME_SERIES_POINTS_NAME,
        # )

        df = self._spark.createDataFrame([], MeasurementsGoldDatabase.TIME_SERIES_POINTS_NAME)
        return TimeSeriesPoints(df)

    def _read_view_or_table(
        self,
        table_name: str,
    ) -> TimeSeriesPoints:
        name = f"{self._catalog_name}.{MeasurementsGoldDatabase.DATABASE_NAME}.{table_name}"
        df = self._spark.read.format("delta").table(name)
        return TimeSeriesPoints(df)
