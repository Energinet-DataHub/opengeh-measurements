from pyspark.sql import DataFrame

import core.gold.infrastructure.config.spark as spark_session
from core.settings import StorageAccountSettings


class CalculatedMeasurementsRepository:
    def __init__(self) -> None:
        self.calculated_schema_name = "measurements_calculated"
        self.calculated_view_name = "hourly_calculated_measurements_v1"
        self.data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT
        self.view = f"{self.calculated_schema_name}.{self.calculated_view_name}"
        self.spark = spark_session.initialize_spark()

    def read_stream(self) -> DataFrame:
        spark = spark_session.initialize_spark()
        return spark.readStream.format("delta").option("ignoreDeletes", "true").table(self.view)
