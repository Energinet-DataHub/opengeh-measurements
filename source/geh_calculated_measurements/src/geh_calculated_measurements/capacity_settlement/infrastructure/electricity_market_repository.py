from geh_common.data_products.electricity_market_measurements_input import capacity_settlement_metering_point_periods_v1
from pyspark.sql import SparkSession

from geh_calculated_measurements.capacity_settlement.domain import MeteringPointPeriods


class ElectricityMarketRepository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def read_metering_point_periods(self) -> MeteringPointPeriods:
        table_name = f"{self._catalog_name}.{capacity_settlement_metering_point_periods_v1.database_name}.{capacity_settlement_metering_point_periods_v1.view_name}"
        df = self._spark.read.format("delta").table(table_name)
        return MeteringPointPeriods(df)
