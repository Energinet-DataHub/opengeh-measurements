import pyspark.sql.types as T

nullable = True

# Observation times where a metering point does have measurement (i.e. where it is not missing)
missing_measurements_log_v1 = T.StructType(
    [
        #
        # GSRN number
        T.StructField("metering_point_id", T.StringType(), not nullable),
        #
        # UTC time
        T.StructField(
            "observation_time",
            T.TimestampType(),
            not nullable,
        ),
        # "missing" | "estimated" | "measured" | "calculated"
        T.StructField("quality", T.StringType(), not nullable),
    ]
)
