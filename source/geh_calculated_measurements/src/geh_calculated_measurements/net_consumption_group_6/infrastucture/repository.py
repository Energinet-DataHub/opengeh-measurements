from geh_common.data_products.electricity_market_measurements_input import (
    net_consumption_group_6_child_metering_points_v1,
    net_consumption_group_6_consumption_metering_point_periods_v1,
)
from pyspark.sql import SparkSession

from geh_calculated_measurements.net_consumption_group_6.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
)


class Repository:
    def __init__(self, spark: SparkSession, catalog_name: str, database_name: str) -> None:
        self._spark = spark
        self._catalog_name = catalog_name
        self._database_name = database_name

    def read_net_consumption_group_6_consumption_metering_point_periods(self) -> ConsumptionMeteringPointPeriods:
        table_name = f"{self._catalog_name}.{self._database_name}.{net_consumption_group_6_consumption_metering_point_periods_v1.view_name}"

        df = self._spark.read.format("delta").table(table_name)
        return ConsumptionMeteringPointPeriods(df)

    def read_net_consumption_group_6_child_metering_points(self) -> ChildMeteringPoints:
        table_name = (
            f"{self._catalog_name}.{self._database_name}.{net_consumption_group_6_child_metering_points_v1.view_name}"
        )

        df = self._spark.read.format("delta").table(table_name)
        return ChildMeteringPoints(df)
