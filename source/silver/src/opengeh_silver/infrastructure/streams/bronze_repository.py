from pyspark.sql import DataFrame, SparkSession

from opengeh_silver.infrastructure.config.bronze_calculated_options import BRONZE_CALCULATED_OPTIONS
from opengeh_silver.infrastructure.config.database_names import DatabaseNames
from opengeh_silver.infrastructure.config.table_names import TableNames
from opengeh_silver.infrastructure.helpers.environment_variable_helper import get_catalog_name


class BronzeRepository:
    def __init__(
        self,
        spark: SparkSession,
    ) -> None:
        self._spark = spark
        self._catalog_name = get_catalog_name()

    def read_calculated_measurements(self) -> DataFrame:
        options = BRONZE_CALCULATED_OPTIONS

        source_table_name = f"{self._catalog_name + '.' if self._catalog_name else ''}{DatabaseNames.bronze}.{TableNames.bronze_calculated_measurements}"
        return self._spark.readStream.format("delta").options(**options).table(source_table_name)
