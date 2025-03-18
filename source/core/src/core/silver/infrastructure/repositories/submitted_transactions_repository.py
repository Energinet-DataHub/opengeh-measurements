from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from pyspark.sql import DataFrame, SparkSession

from core.settings.silver_settings import SilverSettings
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames
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
                f"{SilverMeasurementsColumnNames.orchestration_type} = '{GehCommonOrchestrationType.SUBMITTED.value}'"
            )
        )
