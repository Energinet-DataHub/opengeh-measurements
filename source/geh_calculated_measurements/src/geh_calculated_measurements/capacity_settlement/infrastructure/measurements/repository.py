from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.capacity_settlement.infrastructure.measurements.measurements_gold.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from geh_calculated_measurements.capacity_settlement.infrastructure.measurements.measurements_gold.wrapper import (
    MeteringPointPeriods,
    TimeSeriesPoints,
    metering_point_periods_v1,
    time_series_points_v1,
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
        #    MeasurementsGoldDatabase.TIME_SERIES_POINTS_NAME,capacity_settlement
        # )

        df = self._spark.createDataFrame([], schema=time_series_points_v1)
        return TimeSeriesPoints(df)

    def read_metering_point_periods(self) -> MeteringPointPeriods:
        # TODO: the table does not yet exist in the database
        # df = self._read_view_or_table(
        #    MeasurementsGoldDatabase.TIME_SERIES_POINTS_NAME,
        # )

        df = self._spark.createDataFrame([], schema=metering_point_periods_v1)
        return MeteringPointPeriods(df)

    def _read_view_or_table(
        self,
        table_name: str,
    ) -> DataFrame:
        name = f"{self._catalog_name}.{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{table_name}"
        return self._spark.read.format("delta").table(name)
