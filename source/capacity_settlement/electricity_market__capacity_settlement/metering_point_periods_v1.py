import pyspark.sql.types as t

nullable = True

# Metering point periods for consumption metering points (parent) that has a coupled 'capacity_settlement' metering point (child).
#
# It represents the timeline of the consumption metering points, where the first period (given by period_from_date/period_from_to)
# of each metering point starts when the metering point first time enters 'connected' state - or 'disconnected' if that
# occurs first. A new period starts only when a 'move-in' occurs, and then the previous period is ended at that same time.
#
# Only include rows where:
#   - The period of the parent overlaps (partially of fully) with the period of the child metering point.
#   - The period of the parent ends after 2024-12-31 23:00:00

metering_point_periods_v1 = t.StructType(
    [
        # ID of the consumption metering point (parent)
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # Date when the consumption metering is either (a) entering 'connected'/'disconnected' first time or (b) move-in has occurred.
        # UTC time
        t.StructField("period_from_date", t.TimestampType(), not nullable),
        #
        # Date when the consumption metering point is closed down or a move-in has occurred.
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
