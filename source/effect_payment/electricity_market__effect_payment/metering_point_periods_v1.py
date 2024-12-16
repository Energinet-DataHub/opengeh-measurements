import pyspark.sql.types as t

nullable = True

metering_point_periods_v1 = t.StructType(
    [
        #
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # UTC time
        t.StructField("period_from_date", t.TimestampType(), not nullable),
        #
        # UTC time
        t.StructField("period_to_date", t.TimestampType(), nullable),
        #
        # ID of the child metering point, which is of type 'effect_payment'
        t.StructField("child_metering_point_id", t.StringType(), not nullable),
        #
        # The date where the child metering point (effect_payment) was coupled to the parent metering point
        # UTC time
        t.StructField("coupled_date", t.TimestampType(), not nullable),
        #
        # The date where the child metering point (effect payment) was decoupled from the parent metering point
        # UTC time
        t.StructField("decoupled_date", t.TimestampType(), nullable),
    ]
)
