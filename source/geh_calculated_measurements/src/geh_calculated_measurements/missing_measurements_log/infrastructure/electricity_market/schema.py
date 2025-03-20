import pyspark.sql.types as t

nullable = True

metering_point_periods_v1 = t.StructType(
    [
        # GSRN number
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # The code of the grid area that the metering point belongs to
        t.StructField("grid_area_code", t.StringType(), not nullable),
        #
        # Metering point resolution: PT1H/PT15M
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
Periods for metering points with physical status "connected" or "disconnected". 
Includes all metering point types except those where subtype="calculated" or where type is "internal_use" (D99). 
The periods must be non-overlapping for a given metering point, but their timeline can be split into multiple rows/periods.
"""
