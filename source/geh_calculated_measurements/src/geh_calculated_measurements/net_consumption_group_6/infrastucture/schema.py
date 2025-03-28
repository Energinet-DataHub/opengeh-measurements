import pyspark.sql.types as t

nullable = True
# All periods for every consumption metering point in net settlement group 6.
# Each period is continuos, except when the following changes occur:
# - Energy supplier is changed or is removed.
# - A new customer moves in.
# - Change to electrical heating status.
consumption_metering_point_periods_v1 = t.StructType(
    [
        # GSRN number
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # States whether the metering point has electrical heating in the period.
        t.StructField("has_electrical_heating", t.BooleanType(), not nullable),
        #
        # The settlement month. 1 is January, 12 is December.
        t.StructField("settlement_month", t.IntegerType(), not nullable),
        #
        # UTC time
        # The period start date.
        t.StructField("period_from_date", t.TimestampType(), not nullable),
        #
        # UTC time
        # The period end date.
        t.StructField("period_to_date", t.TimestampType(), nullable),
        #
        # New customer moves in (true) all other changes (false)
        t.StructField("move_in", t.BooleanType(), not nullable),
    ]
)
# All periods for every child metering point in net settlement group 6.
# Only specific child metering point are included.
# - Supply to grid (D06)
# - Consumption from grid (D07)
# - Net consumption (D15)
child_metering_point_periods_v1 = t.StructType(
    [
        # GSRN number.
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # 'supply_to_grid' | 'consumption_from_grid' |'Net_consumption'|
        # The metering point type.
        t.StructField("metering_type", t.StringType(), not nullable),
        #
        # The parent metering point GSRN number.
        t.StructField("parent_metering_point_id", t.StringType(), not nullable),
        #
        # UTC time.
        # The period start date.
        t.StructField("coupled_date", t.TimestampType(), not nullable),
        #
        # UTC time.
        # The period end date.
        t.StructField("uncoupled_date", t.TimestampType(), nullable),
    ]
)
