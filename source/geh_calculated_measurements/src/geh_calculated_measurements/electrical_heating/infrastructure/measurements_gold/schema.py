import pyspark.sql.types as T

nullable = True

# Time series points related to electrical heating.
#
# Points are included when:
# - the unit is kWh
# - the metering point type is one of those listed below
# - the observation time is after 2021-01-01
electrical_heating_v1 = T.StructType(
    [
        #
        # GSRN number
        T.StructField("metering_point_id", T.StringType(), not nullable),
        #
        # 'consumption' | 'supply_to_grid' | 'consumption_from_grid' |
        # 'electrical_heating' | 'net_consumption'
        T.StructField("metering_point_type", T.StringType(), not nullable),
        #
        # UTC time
        T.StructField(
            "observation_time",
            T.TimestampType(),
            not nullable,
        ),
        #
        T.StructField("quantity", T.DecimalType(18, 3), not nullable),
    ]
)
