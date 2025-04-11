from geh_common.pyspark.read_csv import read_csv_path
from pyspark.sql import SparkSession

from geh_calculated_measurements.electrical_heating.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
)


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        electricity_market_data_path: str,
        consumption_metering_point_periods_file_name: str = "consumption_metering_point_periods_v1.csv",
        child_metering_points_file_name: str = "child_metering_points_v1.csv",
    ) -> None:
        self._spark = spark
        self._electricity_market_data_path = electricity_market_data_path
        self._consumption_metering_point_periods_file_name = consumption_metering_point_periods_file_name
        self._child_metering_points_file_name = child_metering_points_file_name

    def read_consumption_metering_point_periods(self) -> ConsumptionMeteringPointPeriods:
        file_path = f"{self._electricity_market_data_path}/{self._consumption_metering_point_periods_file_name}"
        df = read_csv_path(spark=self._spark, path=file_path, schema=ConsumptionMeteringPointPeriods.schema)
        # TODO FIX BY READING FROM DELTA
        return ConsumptionMeteringPointPeriods(df)

    def read_child_metering_points(self) -> ChildMeteringPoints:
        file_path = f"{self._electricity_market_data_path}/{self._child_metering_points_file_name}"
        df = read_csv_path(spark=self._spark, path=file_path, schema=ChildMeteringPoints.schema)
        return ChildMeteringPoints(df)
