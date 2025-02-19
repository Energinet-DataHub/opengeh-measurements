from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.electrical_heating.infrastructure.measurements.calculated_measurements.database_definitions import (
    CalculatedMeasurementsDatabase,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.calculated_measurements.wrapper import (
    CalculatedMeasurementsStorageModel,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.measurements_gold.database_definitions import (
    MeasurementsGoldDatabase,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.measurements_gold.wrapper import (
    TimeSeriesPoints,
    time_series_points_v1,
)


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str | None = None,
    ) -> None:
        self._spark = spark
        self._calculated_measurements_database_name = CalculatedMeasurementsDatabase.DATABASE_NAME
        self._calculated_measurements_table_name = CalculatedMeasurementsDatabase.MEASUREMENTS_NAME
        self._catalog_name = catalog_name
        if self._catalog_name:
            self._calculated_measurements_full_table_path = f"{self._catalog_name}.{self._calculated_measurements_database_name}.{self._calculated_measurements_table_name}"
        else:
            self._calculated_measurements_full_table_path = (
                f"{self._calculated_measurements_database_name}.{self._calculated_measurements_table_name}"
            )

    def write_calculated_measurements(
        self, calculated_measurements: CalculatedMeasurementsStorageModel, write_mode: str = "append"
    ) -> None:
        calculated_measurements.df.write.format("delta").mode(write_mode).saveAsTable(
            self._calculated_measurements_full_table_path
        )

    def read_calculated_measurements(self) -> CalculatedMeasurementsStorageModel:
        return CalculatedMeasurementsStorageModel(self._spark.read.table(self._calculated_measurements_full_table_path))

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
        name = f"{self._catalog_name}.{MeasurementsGoldDatabase.DATABASE_NAME}.{table_name}"
        return self._spark.read.format("delta").table(name)
