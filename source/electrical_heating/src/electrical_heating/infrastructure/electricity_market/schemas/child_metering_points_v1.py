import pyspark.sql.types as t

nullable = True

# Child metering points related to electrical heating.
#
# Periods are included when
# - the metering point is of type
#   'supply_to_grid' 'consumption_from_grid' | 'electrical_heating' | 'net_consumption'
# - the metering point is coupled to a parent metering point
#   Note: The same child metering point cannot be re-coupled after being uncoupled
# - the child metering point physical status is connected or disconnected.
# - the period does not end before 2021-01-01
child_metering_points_v1 = t.StructType(
    [
        #
        # GSRN number
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # 'supply_to_grid' | 'consumption_from_grid' |
        # 'electrical_heating' | 'net_consumption'
        t.StructField("metering_point_type", t.StringType(), not nullable),
        #
        # 'calculated' | 'virtual' | 'physical'
        t.StructField("metering_point_sub_type", t.StringType(), not nullable),
        #
        # GSRN number
        t.StructField("parent_metering_point_id", t.StringType(), not nullable),
        #
        # The date when the child metering point was coupled to the parent metering point
        # UTC time
        t.StructField("coupled_date", t.TimestampType(), not nullable),
        #
        # The date when the child metering point was uncoupled from the parent metering point
        # UTC time
        t.StructField("uncoupled_date", t.TimestampType(), nullable),
    ]
)
