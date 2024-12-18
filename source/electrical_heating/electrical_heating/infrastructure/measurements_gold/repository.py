from pyspark.sql import SparkSession, DataFrame


class MeasurementsGoldRepository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def read_time_series_points(self) -> DataFrame:
        return self._read_view_or_table(
            MeasurementsGoldDatabase.TIME_SERIES_POINTS_NAME,
        )

    def _read_view_or_table(
        self,
        table_name: str,
    ) -> DataFrame:
        name = f"{self._catalog_name}.{MeasurementsGoldDatabase.DATABASE_NAME}.{table_name}"
        return self._spark.read.format("delta").table(name)
