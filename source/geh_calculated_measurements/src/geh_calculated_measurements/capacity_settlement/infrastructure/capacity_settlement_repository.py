from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.database_migrations import DatabaseNames

CALCULATIONS_TABLE_NAME = "capacity_settlement_calculations"
TEN_LARGEST_QUANTITIES_TABLE_NAME = "capacity_settlement_ten_largest_quantities"


class CapacitySettlementRepository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str | None = None,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def _get_full_table_path(self, table_name: str) -> str:
        if self._catalog_name:
            return f"{self._catalog_name}.{DatabaseNames.MEASUREMENTS_CALCULATED_INTERNAL}.{table_name}"
        return f"{database_name}.{table_name}"

    def write_calculations(self, df: DataFrame) -> None:
        table_name = CALCULATIONS_TABLE_NAME
        df.write.format("delta").mode("append").saveAsTable(self._get_full_table_path(table_name))

    def write_ten_largest_quantities(self, df: DataFrame) -> None:
        table_name = TEN_LARGEST_QUANTITIES_TABLE_NAME
        df.write.format("delta").mode("append").saveAsTable(self._get_full_table_path(table_name))
