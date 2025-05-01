from geh_common.data_products.electricity_market_measurements_input import (
    missing_measurements_log_metering_point_periods_v1,
)
from geh_common.testing.dataframes import assert_contract
from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.missing_measurements_log.domain import (
    MeteringPointPeriods,
)


class ElectricityMarketRepository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def read_metering_point_periods(self) -> MeteringPointPeriods:
        table_name = f"{self._catalog_name}.{missing_measurements_log_metering_point_periods_v1.database_name}.{missing_measurements_log_metering_point_periods_v1.view_name}"

        df = self._read_table(table_name)
        assert_contract(df.schema, missing_measurements_log_metering_point_periods_v1.schema)
        return MeteringPointPeriods(df)

    def _read_table(self, table_name: str) -> DataFrame:
        return self._spark.table(table_name)
