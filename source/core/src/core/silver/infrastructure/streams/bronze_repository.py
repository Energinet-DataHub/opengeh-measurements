from pyspark.sql import DataFrame, SparkSession

from core.bronze.domain.constants import BronzeDatabaseNames, BronzeTableNames
from core.silver.infrastructure.config.bronze_calculated_options import BRONZE_CALCULATED_OPTIONS
from core.silver.infrastructure.helpers.environment_variable_helper import get_catalog_name


class BronzeRepository:
    def __init__(
        self,
        spark: SparkSession,
    ) -> None:
        self._spark = spark
        self._catalog_name = get_catalog_name()

    def read_calculated_measurements(self) -> DataFrame:
        options = BRONZE_CALCULATED_OPTIONS

        source_table_name = f"{self._catalog_name + '.' if self._catalog_name else ''}{BronzeDatabaseNames.bronze_database}.{BronzeTableNames.bronze_measurements_table}"
        return self._spark.readStream.format("delta").options(**options).table(source_table_name)
