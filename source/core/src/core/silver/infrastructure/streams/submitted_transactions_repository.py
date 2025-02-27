from pyspark.sql import DataFrame, SparkSession

from core.settings.silver_settings import SilverSettings
from core.silver.domain.constants.col_names_silver_measurements import SilverMeasurementsColNames
from core.silver.domain.constants.orchestration_types import OrchestrationTypes
from core.silver.infrastructure.config import SilverTableNames


class SubmittedTransactionsRepository:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.silver_database_name = SilverSettings().silver_database_name

    def read_submitted_transactions(self) -> DataFrame:
        return (
            self.spark.readStream.format("delta")
            .option("ignoreDeletes", "true")
            .table(f"{self.silver_database_name}.{SilverTableNames.silver_measurements}")
            .filter(
                f"{SilverMeasurementsColNames.orchestration_type} = '{OrchestrationTypes.SUBMITTED_MEASURE_DATA.value}'"
            )
        )
