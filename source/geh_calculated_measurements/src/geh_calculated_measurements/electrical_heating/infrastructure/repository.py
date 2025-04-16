from geh_common.data_products.electricity_market_measurements_input import (
    electrical_heating_child_metering_points_v1,
    electrical_heating_consumption_metering_point_periods_v1,
)
from pyspark.sql import SparkSession

from geh_calculated_measurements.electrical_heating.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
)
from geh_calculated_measurements.net_consumption_group_6.infrastucture.database_definitions import (
    ElectricityMarketMeasurementsInputDatabaseDefinition,
)


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def read_consumption_metering_point_periods(self) -> ConsumptionMeteringPointPeriods:
        table_name = f"{self._catalog_name}.{ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{electrical_heating_consumption_metering_point_periods_v1.view_name}"
        df = self._spark.read.format("delta").table(table_name)
        return ConsumptionMeteringPointPeriods(df)

    def read_child_metering_points(self) -> ChildMeteringPoints:
        table_name = f"{self._catalog_name}.{ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{electrical_heating_child_metering_points_v1.view_name}"
        df = self._spark.read.format("delta").table(table_name)
        return ChildMeteringPoints(df)
