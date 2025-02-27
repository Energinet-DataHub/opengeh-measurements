from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.electrical_heating.domain import TimeSeriesPoints, time_series_points_v1
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.measurements_gold.database_definitions import (
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

    def read_time_series_points(self) -> TimeSeriesPoints:
        # TODO: the table does not yet exist in the database
        # df = self._read_view_or_table(
        #    MeasurementsGoldDatabase.TIME_SERIES_POINTS_NAME,
        # )

        df = self._spark.createDataFrame([], schema=time_series_points_v1)
        return TimeSeriesPoints(df)

    def _read_view_or_table(
        self,
        table_name: str,
    ) -> DataFrame:
        name = f"{self._catalog_name}.{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{table_name}"
        return self._spark.read.format("delta").table(name)
