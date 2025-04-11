from geh_common.data_products.measurements_calculated import calculated_measurements_v1
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.application.model import CalculatedMeasurementsInternal


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str | None = None,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def _get_full_table_path(self) -> str:
        database_name = calculated_measurements_v1.database_name
        table_name = calculated_measurements_v1.view_name
        if self._catalog_name:
            return f"{self._catalog_name}.{database_name}.{table_name}"
        return f"{database_name}.{table_name}"

    def write_calculated_measurements(self, data: CalculatedMeasurementsInternal) -> None:
        df = data.df
        df.write.format("delta").mode("append").saveAsTable(self._get_full_table_path())
