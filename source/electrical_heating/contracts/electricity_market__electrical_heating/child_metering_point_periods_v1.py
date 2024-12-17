import pyspark.sql.types as t

nullable = True

# Child metering points related to electrical heating.
#
# Periods are included when
# - the metering point is of type
#   'supply_to_grid' 'consumption_from_grid' | 'electrical_heating' | 'net_consumption'
# - the metering point is coupled to a parent metering point
# - the child metering point physical status is connected or disconnected.
# - the period ends before 2021-01-01
child_metering_point_periods_v1 = t.StructType(
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
        # "PT15M" | "PT1H"
        t.StructField("resolution", t.StringType(), not nullable),
        #
        # GSRN number
        t.StructField("parent_metering_point_id", t.StringType(), not nullable),
        #
        # See the description of periodization of data above.
        # UTC time
        t.StructField("period_from_date", t.TimestampType(), not nullable),
        #
        # See the description of periodization of data above.
        # UTC time
        t.StructField("period_to_date", t.TimestampType(), nullable),
    ]
)
