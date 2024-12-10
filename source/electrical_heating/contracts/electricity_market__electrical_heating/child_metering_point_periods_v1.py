import pyspark.sql.types as t

nullable = True

# All child metering points related to electrical heating.
#
# The included metering point types are:
# 'supply_to_grid' 'consumption_from_grid' | 'electrical_heating' | 'net_consumption'
child_metering_point_periods_v1 = t.StructType(
    [
        #
        # GRSN number
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # GRSN number
        t.StructField("metering_point_type", t.StringType(), not nullable),
        #
        # GRSN number
        t.StructField("parent_metering_point_id", t.StringType(), not nullable),
        #
        # The date where the metering point was coupled to the parent metering point
        # UTC time
        t.StructField("coupled_date", t.TimestampType(), not nullable),
        #
        # The date where the metering point was decoupled from the parent metering point
        # UTC time
        t.StructField("decoupled_date", t.TimestampType(), nullable),
    ]
)
