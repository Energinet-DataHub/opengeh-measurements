import pyspark.sql.types as t

nullable = True

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
        # The date where the child metering point (of type 'capacity_settlement') was created
        # UTC time
        t.StructField("child_period_from_date", t.TimestampType(), not nullable),
        #
        # The date where the child metering point (of type 'capacity_settlement') was closed down
        # UTC time
        t.StructField("child_period_to_date", t.TimestampType(), nullable),
    ]
)
"""
Metering point periods for consumption metering points (parent) that have a coupled 'capacity_settlement' metering point (child).

It represents the timeline of the consumption metering points. The first period (given by period_from_date/period_from_to)
of each metering point starts when the metering point first time enters 'connected' state - or 'disconnected' if that
occurs first. After that, new period starts when (and only when) a 'move-in' occurs, and the previous period is then
terminated at that same time.

Exclude rows where the period of the parent
- does not have any overlap with the period of the child metering point.
- ends before 2024-12-31 23:00:00

Formatting is according to ADR-144 with the following constraints:
- No column may use quoted values
- All date/time values must include seconds
"""
