from geh_common.testing.dataframes import assert_contract
from pyspark.sql import SparkSession

from geh_calculated_measurements.net_consumption_group_6.domain import (
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
        catalog_name: str | None = None,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def read_net_consumption_group_6_consumption_metering_point_periods(self) -> ConsumptionMeteringPointPeriods:
        table_name = f"{self._catalog_name}.{ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CONSUMPTION_METERING_POINT_PERIODS}"

        df = self._spark.read.format("delta").table(table_name)
        assert_contract(df.schema, ConsumptionMeteringPointPeriods.schema)
        return ConsumptionMeteringPointPeriods(df)

    def read_net_consumption_group_6_child_metering_point_periods(self) -> ChildMeteringPoints:
        table_name = f"{self._catalog_name}.{ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINT}"

        df = self._spark.read.format("delta").table(table_name)
        assert_contract(df.schema, ChildMeteringPoints.schema)
        return ChildMeteringPoints(df)
