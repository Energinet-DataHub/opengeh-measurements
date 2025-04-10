import pyspark.sql.types as T
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition, Table


class MissingMeasurementsLogTable(Table):
    def __init__(self, catalog_name: str) -> None:
        self.fully_qualified_name = f"{catalog_name}.{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MISSING_MEASUREMENTS_LOG_TABLE_NAME}"
        super().__init__()

    orchestration_instance_id = T.StructField(ContractColumnNames.orchestration_instance_id, T.StringType(), False)
    metering_point_id = T.StructField(ContractColumnNames.metering_point_id, T.StringType(), False)
    date = T.StructField(ContractColumnNames.date, T.DateType(), False)

    def read(self) -> DataFrame:
        return self.spark.table(self.fully_qualified_name)
