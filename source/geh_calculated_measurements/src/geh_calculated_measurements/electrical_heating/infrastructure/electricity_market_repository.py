from geh_common.data_products.electricity_market_measurements_input import (
    electrical_heating_child_metering_points_v1,
    electrical_heating_consumption_metering_point_periods_v1,
)
from geh_common.testing.dataframes import assert_contract
from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.electrical_heating.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
)


class ElectricityMarketRepository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def read_consumption_metering_point_periods(self) -> ConsumptionMeteringPointPeriods:
        table_name = f"{self._catalog_name}.{electrical_heating_consumption_metering_point_periods_v1.database_name}.{electrical_heating_consumption_metering_point_periods_v1.view_name}"
        df = self._read_table(table_name)
        assert_contract(df.schema, electrical_heating_consumption_metering_point_periods_v1.schema)
        return ConsumptionMeteringPointPeriods(df)

    def read_child_metering_points(self) -> ChildMeteringPoints:
        table_name = f"{self._catalog_name}.{electrical_heating_child_metering_points_v1.database_name}.{electrical_heating_child_metering_points_v1.view_name}"
        df = self._read_table(table_name)
        assert_contract(df.schema, electrical_heating_child_metering_points_v1.schema)
        return ChildMeteringPoints(df)

    def _read_table(self, table_name: str) -> DataFrame:
        return self._spark.read.format("delta").table(table_name)
