import pyspark.sql.types as t

nullable = True


metering_point_periods_v1 = t.StructType(
    [
        t.StructField("metering_point_id", t.StringType(), not nullable),
        t.StructField("period_from_date", t.TimestampType(), not nullable),
        t.StructField("period_to_date", t.TimestampType(), nullable),
        t.StructField("child_metering_point_id", t.StringType(), not nullable),
        t.StructField("child_period_from_date", t.TimestampType(), not nullable),
        t.StructField("child_period_to_date", t.TimestampType(), nullable),
    ]
)
