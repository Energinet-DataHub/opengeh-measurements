from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.common.infrastructure.calculated_measurements.database_definitions import (
    CalculatedMeasurementsInternalDatabaseDefinition,
)


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str | None = None,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def _get_full_table_path(self, table_name: str) -> str:
        database_name = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
        if self._catalog_name:
            return f"{self._catalog_name}.{database_name}.{table_name}"
        return f"{database_name}.{table_name}"

    def write_calculations(self, df: DataFrame) -> None:
        table_name = CalculatedMeasurementsInternalDatabaseDefinition.CAPACITY_SETTLEMENT_CALCULATIONS_TABLE_NAME
        df.write.format("delta").mode("append").saveAsTable(self._get_full_table_path(table_name))

    def write_ten_largest_quantities(self, df: DataFrame) -> None:
        table_name = (
            CalculatedMeasurementsInternalDatabaseDefinition.CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES_TABLE_NAME
        )
        df.write.format("delta").mode("append").saveAsTable(self._get_full_table_path(table_name))
