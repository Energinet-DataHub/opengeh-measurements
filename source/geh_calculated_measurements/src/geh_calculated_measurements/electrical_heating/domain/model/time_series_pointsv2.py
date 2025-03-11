import pyspark.sql.types as t
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain.model.table import Table


class TimeSeriesPointsV2(Table):
    def __init__(self, df: DataFrame) -> None:
        super().__init__(df)

    metering_point_id = t.StructField("metering_point_id", t.StringType(), not Table.nullable)
    observation_time = t.StructField("observation_time", t.TimestampType(), not Table.nullable)
    quantity = t.StructField("quantity", t.DecimalType(18, 3), not Table.nullable)
