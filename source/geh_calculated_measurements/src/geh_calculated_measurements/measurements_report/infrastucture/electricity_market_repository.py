from pyspark.sql import SparkSession


class ElectricityMarketRepository:
    def __init__(self, spark: SparkSession, catalog_name: str) -> None:
        self._spark = spark
        self._catalog_name = catalog_name
