import pyspark.sql.types as T

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.infrastructure.current_measurements.table import Table
from geh_calculated_measurements.missing_measurements_log.infrastructure.database_definitions import (
    MeteringPointPeriodsDatabaseDefinition,
)


class MeteringPointPeriodsTable(Table):
    def __init__(self, catalog_name: str) -> None:
        self.fully_qualified_name = f"{catalog_name}.{MeteringPointPeriodsDatabaseDefinition.DATABASE_NAME}.{MeteringPointPeriodsDatabaseDefinition.METERING_POINT_PERIODS}"

    metering_point_id = T.StructField(ContractColumnNames.metering_point_id, T.StringType(), False)
    grid_area_code = T.StructField(ContractColumnNames.grid_area_code, T.StringType(), False)
    resolution = T.StructField(ContractColumnNames.resolution, T.StringType(), False)
    period_from_date = T.StructField(ContractColumnNames.period_from_date, T.TimestampType(), False)
    period_to_date = T.StructField(ContractColumnNames.period_to_date, T.TimestampType(), True)
