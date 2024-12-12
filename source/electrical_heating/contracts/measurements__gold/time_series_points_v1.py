import pyspark.sql.types as t

nullable = True


# All time series points related to electrical heating.
time_series_points_v1 = t.StructType(
    [
        #
        # GSRN number
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
        # 'consumption' | 'supply_to_grid' | 'consumption_from_grid' |
        # 'electrical_heating' | 'net_consumption'
        t.StructField("metering_point_type", t.StringType(), not nullable),
    ]
)
