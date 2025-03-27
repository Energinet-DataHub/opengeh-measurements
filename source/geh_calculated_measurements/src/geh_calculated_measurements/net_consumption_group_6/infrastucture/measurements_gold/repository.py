from pyspark.sql import SparkSession

from geh_calculated_measurements.net_consumption_group_6.domain import TimeSeriesPoints
from geh_calculated_measurements.net_consumption_group_6.infrastucture.measurements_gold.database_definitions import (
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
        table_name = f"{self._catalog_name}.{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}"

        df = self._spark.read.format("delta").table(table_name)

        return TimeSeriesPoints(df)
