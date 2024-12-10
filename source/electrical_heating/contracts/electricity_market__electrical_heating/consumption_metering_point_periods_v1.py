import pyspark.sql.types as t

nullable = True

consumption_metering_point_periods_v1 = t.StructType(
    [
        #
        # GRSN number
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        #
        t.StructField("has_electrical_heating", t.BooleanType(), not nullable),
        #
        # 2 | 3 | 6 | NULL
        t.StructField("net_settlement_group", t.IntegerType(), not nullable),
        #
        # The number of the month. 1 is January, 12 is December.
        t.StructField(
            "net_settlement_group_scheduled_month_meter_reading",
            t.IntegerType(),
            nullable,
        ),
        #
        # UTC time
        t.StructField("period_from_date", t.TimestampType(), not nullable),
        #
        # UTC time
        t.StructField("period_to_date", t.TimestampType(), nullable),
    ]
)
