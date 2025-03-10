from pyspark.sql import SparkSession

from geh_calculated_measurements.electrical_heating.domain import TimeSeriesPoints
from geh_calculated_measurements.electrical_heating.domain.model.time_series_pointsv2 import TimeSeriesPointsV2
from geh_calculated_measurements.electrical_heating.infrastructure.measurements_gold.database_definitions import (
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


class RepositoryV2:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str | None = None,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def read_time_series_points(self) -> TimeSeriesPointsV2:
        table_name = f"{self._catalog_name}.{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME}"
        df = self._spark.read.format("delta").table(table_name)
        return TimeSeriesPointsV2(df)
