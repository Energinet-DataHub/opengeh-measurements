from geh_common.pyspark.read_csv import read_csv_path
from pyspark.sql import SparkSession

from geh_calculated_measurements.electrical_heating.domain import (
    ConsumptionMeteringPointPeriods,
)


class Repository:
    def __init__(
        self,
        spark: SparkSession,,
    ) -> None:
        self._spark = spark
        self._electricity_market_data_path = electricity_market_data_path
        self._consumption_metering_point_periods_file_name = consumption_metering_point_periods_file_name
        self._child_metering_points_file_name = child_metering_points_file_name

    def read_metering_point_periods(self) -> ConsumptionMeteringPointPeriods:
        file_path = f"{self._electricity_market_data_path}/{self._consumption_metering_point_periods_file_name}"
        df = read_csv_path(spark=self._spark, path=file_path, schema=ConsumptionMeteringPointPeriods.schema)
        return MeteringPointPeriods(df)
