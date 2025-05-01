from geh_common.data_products.electricity_market_measurements_input import capacity_settlement_metering_point_periods_v1
from geh_common.pyspark.read_csv import read_csv_path
from pyspark.sql import SparkSession

from geh_calculated_measurements.capacity_settlement.domain import MeteringPointPeriods


class ElectricityMarketRepository:
    def __init__(
        self,
        spark: SparkSession,
        electricity_market_data_path: str,
    ) -> None:
        self._spark = spark
        self._electricity_market_data_path = electricity_market_data_path

    def read_metering_point_periods(self) -> MeteringPointPeriods:
        file_path = f"{self._electricity_market_data_path}/metering_point_periods_v1.csv"
        df = read_csv_path(
            spark=self._spark, path=file_path, schema=capacity_settlement_metering_point_periods_v1.schema
        )
        return MeteringPointPeriods(df)
