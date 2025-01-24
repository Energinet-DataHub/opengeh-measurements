from pyspark.sql import DataFrame, SparkSession

from silver.infrastructure.bronze.database_names import DatabaseNames
from silver.infrastructure.bronze.options import BRONZE_CALCULATED_OPTIONS
from silver.infrastructure.bronze.table_names import TableNames
from silver.infrastructure.utils.env_vars_utils import get_catalog_name


class Repository:
    def __init__(
        self,
        spark: SparkSession,
    ) -> None:
        self._spark = spark
        self._catalog_name = get_catalog_name()

    def read_calculated_measurements(self) -> DataFrame:
        options = BRONZE_CALCULATED_OPTIONS
        source_table_name = f"{self._catalog_name + '.' if self._catalog_name else ''}{DatabaseNames.bronze_database}.{TableNames.bronze_calculated_measurements_table}"
        return self._spark.readStream.format("delta").options(**options).table(source_table_name)
