from pyspark.sql import DataFrame

import core.gold.infrastructure.config.spark as spark_session
from core.gold.infrastructure.config.external_database_names import ExternalDatabaseNames
from core.gold.infrastructure.config.external_view_names import ExternalViewNames
from core.settings import StorageAccountSettings


class CalculatedMeasurementsRepository:
    def __init__(self) -> None:
        calculated_schema_name = ExternalDatabaseNames.calculated
        calculated_view_name = ExternalViewNames.hourly_calcualted_measurements_v1
        self.view = f"{calculated_schema_name}.{calculated_view_name}"
        self.spark = spark_session.initialize_spark()

    def read_stream(self) -> DataFrame:
        spark = spark_session.initialize_spark()
        return spark.readStream.format("delta").option("ignoreDeletes", "true").table(self.view)
