import pyspark.sql.types as t

nullable = True


_point = t.StructType(
    [
        #
        # A sequential numbering.
        # Combined with start and resolution it can be used to determine the observation time
        # of the individual points in the transaction.
        t.StructField("position", t.StringType(), not nullable),
        #
        # The quantity calculated by the electrical heating job
        t.StructField("quantity", t.StringType(), not nullable),
        #
        # Must be: "calculated"
        t.StructField("quality", t.StringType(), not nullable),
    ]
)

# Electrical heating result is delivered to Measurements
# as one transaction per metering point id.
measurements_bronze_v1 = t.StructType(
    [
        #
        # Must be: "electrical_heating"
        t.StructField("orchestration_type", t.StringType(), not nullable),
        #
        # The GUIDvalue of the electrical heating calculation job
        t.StructField("orchestration_instance_id", t.StringType(), not nullable),
        #
        # Metering Point ID
        # GSRN number
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # Transaction ID. Created by the electrical heating calculation job.
        t.StructField("transaction_id", t.StringType(), not nullable),
        #
        # A DateTime value indicating when the transaction was created
        # by the electrical heating calculation job.
        t.StructField("transaction_creation_datetime", t.StringType(), not nullable),
        #
        # The start DateTime of the transaction (the first position’s observation time)
        t.StructField("start_datetime", t.TimestampType(), not nullable),
        #
        # The end DateTime of the transaction
        t.StructField("end_datetime", t.TimestampType(), not nullable),
        #
        # Must be "electrical_heating"
        t.StructField("metering_point_type", t.StringType(), not nullable),
        #
        # Must be "Active Energy"
        t.StructField("product", t.StringType(), not nullable),
        #
        # Metering Unit
        # Must be: "kWh"
        t.StructField("unit", t.StringType(), not nullable),
        #
        # Must be: "PT1H"
        t.StructField("resolution", t.StringType(), not nullable),
        #
        # Points
        t.StructField(
            "points",
            t.ArrayType(_point, containsNull=False),
            not nullable,
        ),
    ]
)
