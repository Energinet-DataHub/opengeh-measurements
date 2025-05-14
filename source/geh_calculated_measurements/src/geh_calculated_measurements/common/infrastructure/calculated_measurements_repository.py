from pyspark.sql import SparkSession

from geh_calculated_measurements.common.application.model import CalculatedMeasurementsInternal
from geh_calculated_measurements.database_migrations import DatabaseNames

TABLE_NAME = "calculated_measurements"


class CalculatedMeasurementsRepository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str | None = None,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def _get_full_table_path(self, database_name: str, table_name: str) -> str:
        if self._catalog_name:
            return f"{self._catalog_name}.{DatabaseNames.MEASUREMENTS_CALCULATED_INTERNAL}.{table_name}"
        return f"{database_name}.{table_name}"

    def write_calculated_measurements(self, data: CalculatedMeasurementsInternal) -> None:
        df = data.df
        df.write.format("delta").mode("append").saveAsTable(
            self._get_full_table_path(DatabaseNames.MEASUREMENTS_CALCULATED_INTERNAL, TABLE_NAME)
        )

    def read_calculated_measurements(self) -> CalculatedMeasurementsInternal:
        table_name = self._get_full_table_path(DatabaseNames.MEASUREMENTS_CALCULATED_INTERNAL, TABLE_NAME)
        df = self._spark.read.format("delta").table(table_name)

        return CalculatedMeasurementsInternal(df)
