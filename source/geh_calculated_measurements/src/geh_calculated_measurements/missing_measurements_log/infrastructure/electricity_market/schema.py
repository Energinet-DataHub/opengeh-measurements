import pyspark.sql.types as t

nullable = True

metering_point_periods_v1 = t.StructType(
    [
        #
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        t.StructField("grid_area_code", t.StringType(), not nullable),
        #
        t.StructField("resolution", t.StringType(), not nullable),
        #
        # UTC time
        t.StructField("period_from_date", t.TimestampType(), not nullable),
        #
        # UTC time
        t.StructField("period_to_date", t.TimestampType(), nullable),
    ]
)
"""
Metering point periods for all metering points that grid access providers submit measurements for. This includes all metering points
except those where subtype='calculated' or where type="D99".
"""
