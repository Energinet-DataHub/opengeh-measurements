from pyspark.sql import DataFrame, SparkSession


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def save(self, calculation: DataFrame) -> None:
        # TODO Will implemented in another PR.
        pass
