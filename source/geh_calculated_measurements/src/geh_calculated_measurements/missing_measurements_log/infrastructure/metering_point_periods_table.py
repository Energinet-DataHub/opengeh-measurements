import pyspark.sql.types as T
from geh_common.data_products.electricity_market_measurements_input import (
    missing_measurements_log_metering_point_periods_v1,
)
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.infrastructure import Table


class MeteringPointPeriodsTable(Table):
    """Represents periods for metering points with physical status "connected" or "disconnected".

    Includes all metering point types except those where subtype="calculated" or where type is "internal_use" (D99).
    The periods must be non-overlapping for a given metering point, but their timeline can be split into multiple rows/periods.
    """

    def __init__(self, catalog_name: str, database_name: str) -> None:
        self.fully_qualified_name = (
            f"{catalog_name}.{database_name}.{missing_measurements_log_metering_point_periods_v1.view_name}"
        )
        super().__init__()

    metering_point_id = T.StructField(ContractColumnNames.metering_point_id, T.StringType(), False)
    grid_area_code = T.StructField(ContractColumnNames.grid_area_code, T.StringType(), False)
    resolution = T.StructField(ContractColumnNames.resolution, T.StringType(), False)
    period_from_date = T.StructField(ContractColumnNames.period_from_date, T.TimestampType(), False)
    period_to_date = T.StructField(ContractColumnNames.period_to_date, T.TimestampType(), True)

    def read(self) -> DataFrame:
        return self.spark.table(self.fully_qualified_name)
