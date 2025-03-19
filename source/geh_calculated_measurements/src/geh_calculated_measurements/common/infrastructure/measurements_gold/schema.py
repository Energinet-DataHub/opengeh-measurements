import pyspark.sql.types as T

nullable = True

"""
Schema for all current measurements. This is a generic contract used for multiple calculations
"""
current_measurements_schema = T.StructType(
    [
        # GSRN (18 characters) that uniquely identifies the metering point
        # Example: 578710000000000103
        T.StructField("metering_point_id", T.StringType(), not nullable),
        # Energy quantity in kWh for the given observation time.
        # Null when quality is missing.
        # Example: 1234.534217
        T.StructField("quantity", T.DecimalType(18, 3), not nullable),
        # "missing" | "estimated" | "measured" | "calculated"
        # Example: measured
        T.StructField("quality", T.StringType(), not nullable),
        # UTC time
        T.StructField("observation_time", T.TimestampType(), not nullable),
    ]
)

