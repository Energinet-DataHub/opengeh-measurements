import pyspark.sql.types as T
from geh_common.data_products.measurements_core.measurements_gold import current_v1
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.infrastructure.current_measurements.table import Table


class CurrentMeasurementsTable(Table):
    def __init__(self, catalog_name: str) -> None:
        self.fully_qualified_name = f"{catalog_name}.{current_v1.database_name}.{current_v1.view_name}"
        super().__init__()

    metering_point_id = T.StructField(ContractColumnNames.metering_point_id, T.StringType(), False)
    observation_time = T.StructField(ContractColumnNames.observation_time, T.TimestampType(), False)
    quantity = T.StructField(ContractColumnNames.quantity, T.DecimalType(18, 3), False)
    quality = T.StructField(ContractColumnNames.quality, T.StringType(), False)
    metering_point_type = T.StructField(ContractColumnNames.metering_point_type, T.StringType(), False)

    def read(self) -> DataFrame:
        return self.spark.table(self.fully_qualified_name)
