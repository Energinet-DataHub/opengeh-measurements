from pyspark.sql import SparkSession

from geh_calculated_measurements.capacity_settlement.domain import TimeSeriesPoints
from geh_calculated_measurements.capacity_settlement.infrastructure.measurements_gold.database_definitions import (
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
        name = f"{self._catalog_name}.{MeasurementsGoldDatabaseDefinition().DATABASE_MEASUREMENTS_GOLDS}.{MeasurementsGoldDatabaseDefinition().MEASUREMENTS}"
        df = self._spark.read.format("delta").table(name)
        return TimeSeriesPoints(df)
