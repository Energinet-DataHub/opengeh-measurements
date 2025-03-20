import pyspark.sql.types as T

nullable = True

metering_point_periods_v1 = T.StructType(
    [
        # GSRN number
        T.StructField("metering_point_id", T.StringType(), not nullable),
        #
        # The code of the grid area that the metering point belongs to
        T.StructField("grid_area_code", T.StringType(), not nullable),
        #
        # Metering point resolution: PT1H/PT15M
        T.StructField("resolution", T.StringType(), not nullable),
        #
        # UTC time
        T.StructField("period_from_date", T.TimestampType(), not nullable),
        #
        # UTC time
        T.StructField("period_to_date", T.TimestampType(), nullable),
    ]
)
"""
Periods for metering points with physical status "connected" or "disconnected". 
Includes all metering point types except those where subtype="calculated" or where type is "internal_use" (D99). 
The periods must be non-overlapping for a given metering point, but their timeline can be split into multiple rows/periods.
"""
