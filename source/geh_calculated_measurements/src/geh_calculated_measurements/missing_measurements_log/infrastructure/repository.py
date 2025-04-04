from geh_common.testing.dataframes import assert_contract
from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.missing_measurements_log.domain import MeteringPointPeriods
from geh_calculated_measurements.missing_measurements_log.infrastructure.database_definitions import (
    MeteringPointPeriodsDatabaseDefinition,
)


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def read_metering_point_periods(self) -> MeteringPointPeriods:
        df = self._read()
        assert_contract(df.schema, MeteringPointPeriods.schema)
        return MeteringPointPeriods(df)

    def _read(self) -> DataFrame:
        """Read table or view. The function is introduced to allow mocking in tests."""
        database_name = MeteringPointPeriodsDatabaseDefinition.DATABASE_NAME
        table_name = MeteringPointPeriodsDatabaseDefinition.METERING_POINT_PERIODS
        return self._spark.read.table(f"{self._catalog_name}.{database_name}.{table_name}")
