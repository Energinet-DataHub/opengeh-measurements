from pyspark.sql import SparkSession, DataFrame


class MeasurementsRepository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def read_consumption_metering_point_periods(self) -> DataFrame:
        return self._read_view_or_table(
            MeasurementsDatabase.CONSUMPTION_METERING_POINT_PERIODS_NAME,
        )

    def _read_view_or_table(
        self,
        table_name: str,
    ) -> DataFrame:
        name = f"{self._catalog_name}.{ElectricityMarketDatabase.DATABASE_NAME}.{table_name}"
        return self._spark.read.format("delta").table(name)
