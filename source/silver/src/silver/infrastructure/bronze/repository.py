from pyspark.sql import SparkSession
from source.silver.src.silver.infrastructure.bronze.database_names import DatabaseNames
from source.silver.src.silver.infrastructure.bronze.table_names import TableNames
from source.silver.src.silver.infrastructure.bronze.options import BRONZE_CALCULATED_OPTIONS

class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name        

    def read_calculated_measurements(self) -> None:
        options = BRONZE_CALCULATED_OPTIONS
        source_table_name = f"{self._catalog_name}.{DatabaseNames.bronze_database}.{TableNames.bronze_calculated_table}"
        return self._spark.readStream.format("delta").options(**options).table(source_table_name)
