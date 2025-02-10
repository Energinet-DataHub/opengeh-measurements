import pyspark.sql.types as t

nullable = True

# View name
electrical_heating_view_v1 = t.StructType(
    [
        t.StructField("metering_point_id", t.StringType(), nullable),
        t.StructField("quantity", t.DecimalType(18, 3), nullable),
        t.StructField("observation_time", t.TimestampType(), nullable),
        t.StructField("metering_point_type", t.StringType(), nullable),
    ]
)
