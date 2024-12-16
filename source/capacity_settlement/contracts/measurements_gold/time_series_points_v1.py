import pyspark.sql.types as t

nullable = True


# All time series points related to capacity settlement.
time_series_points_v1 = t.StructType(
    [
        #
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        t.StructField("quantity", t.DecimalType(18, 3), not nullable),
        #
        # UTC time
        t.StructField(
            "observation_time",
            t.TimestampType(),
            not nullable,
        ),
        #
        # 'consumption' | 'capacity_settlement'
        t.StructField("metering_point_type", t.StringType(), not nullable),
    ]
)
