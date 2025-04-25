from pyspark.sql import SparkSession

from geh_calculated_measurements.electrical_heating.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
)
from tests.external_data_products import ExternalDataProducts


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def read_consumption_metering_point_periods(self) -> ConsumptionMeteringPointPeriods:
        consumption_metering_point_periods = ExternalDataProducts.ELECTRICAL_HEATING_CONSUMPTION_METERING_POINT_PERIODS
        table_name = f"{self._catalog_name}.{consumption_metering_point_periods.database_name}.{consumption_metering_point_periods.view_name}"
        df = self._spark.read.format("delta").table(table_name)
        return ConsumptionMeteringPointPeriods(df)

    def read_child_metering_points(self) -> ChildMeteringPoints:
        child_metering_points = ExternalDataProducts.ELECTRICAL_HEATING_CHILD_METERING_POINTS
        table_name = f"{self._catalog_name}.{child_metering_points.database_name}.{child_metering_points.view_name}"
        df = self._spark.read.format("delta").table(table_name)
        return ChildMeteringPoints(df)
