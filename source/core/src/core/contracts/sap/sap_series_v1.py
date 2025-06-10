import pyspark.sql.types as t

nullable = True

database_name = "measurements_gold"

view_name = "sap_series_v1"

schema = t.StructType(
    [
        t.StructField("metering_point_id", t.StringType(), not nullable),
        t.StructField("transaction_id", t.StringType(), not nullable),
        t.StructField("transaction_creation_datetime", t.TimestampType(), not nullable),
        t.StructField("start_time", t.TimestampType(), not nullable),
        t.StructField("end_time", t.TimestampType(), not nullable),
        t.StructField("unit", t.StringType(), not nullable),
        t.StructField("resolution", t.StringType(), not nullable),
        t.StructField("created", t.TimestampType(), not nullable),
    ]
)
