from pyspark.sql import SparkSession

from geh_calculated_measurements.missing_measurements_log.domain import MeteringPointPeriods
from geh_calculated_measurements.missing_measurements_log.infrastructure.database_definitions import (
    MeteringPointPeriodsDatabaseDefinition,
)


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str | None = None,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def read_metering_point_periods(self) -> MeteringPointPeriods:
        table_name = f"{self._catalog_name}.{MeteringPointPeriodsDatabaseDefinition.DATABASE_NAME}.{MeteringPointPeriodsDatabaseDefinition.METERING_POINT_PERIODS}"

        df = self._spark.read.format("delta").table(table_name)

        return MeteringPointPeriods(df)
