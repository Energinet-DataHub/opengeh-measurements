import pyspark.sql.types as t

nullable = True


# Time series points related to electrical heating.
#
# Points are included when:
# - the unit is kWh
# - the metering point type is one of those listed below
# - the observation time is after 2021-01-01
time_series_points_v1 = t.StructType(
    [
        #
        # GSRN number
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # 'consumption' | 'supply_to_grid' | 'consumption_from_grid' |
        # 'electrical_heating' | 'net_consumption'
        t.StructField("metering_point_type", t.StringType(), not nullable),
        #
        # UTC time
        t.StructField(
            "observation_time",
            t.TimestampType(),
            not nullable,
        ),
        #
        t.StructField("quantity", t.DecimalType(18, 3), not nullable),
    ]
)

metering_point_periods_v1 = t.StructType(
    [
        t.StructField("metering_point_id", t.StringType(), not nullable),
        t.StructField("period_from_date", t.TimestampType(), not nullable),
        t.StructField("period_to_date", t.TimestampType(), not nullable),
        t.StructField("child_metering_point_id", t.StringType(), not nullable),
        t.StructField("child_period_from_date", t.StringType(), not nullable),
        t.StructField("child_period_to_date", t.StringType(), not nullable),
    ]
)
