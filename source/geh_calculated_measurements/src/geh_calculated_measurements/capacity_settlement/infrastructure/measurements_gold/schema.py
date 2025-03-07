import pyspark.sql.types as t

nullable = True


# Time series points related to capacity settlement.
#
# Points are included when:
# - the unit is kWh
# - the metering point type is one of those listed below
capacity_settlement_v1 = t.StructType(
    [
        #
        # GSRN number
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # 'consumption' | 'capcity_settlement'
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
