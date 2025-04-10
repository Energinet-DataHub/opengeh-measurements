from pyspark.sql import DataFrame

import core.gold.infrastructure.config.spark as spark_session
from core.gold.infrastructure.config.external_view_names import ExternalViewNames
from core.settings.calculated_settings import CalculatedSettings


class CalculatedMeasurementsRepository:
    def __init__(self) -> None:
        calculated_database_name = CalculatedSettings().calculated_database_name
        calculated_view_name = ExternalViewNames.calculated_measurements_v1
        self.view = f"{calculated_database_name}.{calculated_view_name}"
        self.spark = spark_session.initialize_spark()

    def read_stream(self) -> DataFrame:
        return (
            self.spark.readStream.format("delta")
            .option("ignoreDeletes", "true")
            .option("skipChangeCommits", "true")
            .table(self.view)
        )
