import pyspark.sql.types as t

nullable = True

# Metering point periods for consumption metering points (parent) with a coupled 'capacity_settlement' metering point (child).
# Only include rows where the period of the parent overlaps (partially of fully) with the period of the child metering point.
# A period is created if - and only if - the consumption metering point is connected or a move-in has occurred.

metering_point_periods_v1 = t.StructType(
    [
        # ID of the consumption metering point (parent)
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # Date when the consumption metering point is connected or a move-in has occurred.
        # UTC time
        t.StructField("period_from_date", t.TimestampType(), not nullable),
        #
        # Date when the consumption metering point is disconnected or a move-in has occurred.
        # UTC time
        t.StructField("period_to_date", t.TimestampType(), nullable),
        #
        # ID of the child metering point, which is of type 'capacity_settlement'
        t.StructField("child_metering_point_id", t.StringType(), not nullable),
        #
        # The date where the child metering point (of type 'capacity_settlement') was coupled to the parent metering point
        # UTC time
        t.StructField("coupled_date", t.TimestampType(), not nullable),
        #
        # The date where the child metering point (of type 'capacity_settlement') was decoupled from the parent metering point
        # UTC time
        t.StructField("decoupled_date", t.TimestampType(), nullable),
    ]
)
