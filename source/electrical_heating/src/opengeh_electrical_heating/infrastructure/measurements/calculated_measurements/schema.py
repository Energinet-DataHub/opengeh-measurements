import pyspark.sql.types as t

nullable = True


point = t.StructType(
    [
        #
        # A sequential numbering. Always starting at 1.
        # Combined with start_datetime and resolution it can be used to determine the observation time
        # of the individual points in the transaction.
        t.StructField("position", t.StringType(), not nullable),
        #
        # The quantity calculated by the electrical heating job
        t.StructField("quantity", t.DecimalType(18, 3), not nullable),
        #
        # Must be: "calculated"
        t.StructField("quality", t.StringType(), not nullable),
    ]
)

# Electrical heating result is delivered to Measurements
# as one transaction per metering point id.
calculated_measurements_schema = t.StructType(
    [
        #
        # Must be: "electrical_heating"
        t.StructField("orchestration_type", t.StringType(), not nullable),
        #
        # The GUID value of the electrical heating calculation job
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
        t.StructField("transaction_creation_datetime", t.TimestampType(), not nullable),
        #
        # Must be "electrical_heating"
        t.StructField("metering_point_type", t.StringType(), not nullable),
        #
        # The product must be the code for "active energy"
        # Must be: 8716867000030
        t.StructField("product", t.StringType(), not nullable),
        #
        # Metering Unit
        # Must be: "kWh"
        t.StructField("unit", t.StringType(), not nullable),
        #
        # Must be: "PT1H"
        t.StructField("resolution", t.StringType(), not nullable),
        #
        # The start DateTime of the transaction (the first position’s observation time)
        t.StructField("start_datetime", t.TimestampType(), not nullable),
        #
        # The end DateTime of the transaction
        t.StructField("end_datetime", t.TimestampType(), not nullable),
        #
        # Points
        t.StructField(
            "points",
            t.ArrayType(point, containsNull=False),
            not nullable,
        ),
    ]
)
