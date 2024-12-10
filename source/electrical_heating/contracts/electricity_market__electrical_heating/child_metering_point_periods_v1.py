import pyspark.sql.types as t

nullable = True

child_metering_point_periods_v1 = t.StructType(
    [
        #
        # GRSN number
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # GRSN number
        t.StructField("metering_point_type", t.StringType(), not nullable),
        #
        # GRSN number
        t.StructField("parent_metering_point_id", t.StringType(), not nullable),
        #
        # UTC time
        t.StructField("period_from_date", t.TimestampType(), not nullable),
        #
        # UTC time
        t.StructField("period_to_date", t.TimestampType(), nullable),
    ]
)
