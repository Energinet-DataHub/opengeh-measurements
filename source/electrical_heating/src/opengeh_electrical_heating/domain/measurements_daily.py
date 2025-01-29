import pyspark.sql.types as T
from pyspark.sql import DataFrame


class MeasurementsDaily:
    def __init__(self, df: DataFrame):
        columns = [field.name for field in measurements_daily_schema.fields]
        df = df.select(columns)


measurements_daily_schema = T.StructType(
    [
        T.StructField("metering_point_id", T.StringType(), False),
        T.StructField("date", T.TimestampType(), False),
        T.StructField("quantity", T.DecimalType(), False),
    ]
)
