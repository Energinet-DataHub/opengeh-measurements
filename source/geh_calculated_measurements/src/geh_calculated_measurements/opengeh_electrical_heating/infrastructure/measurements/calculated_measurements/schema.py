import pyspark.sql.types as t

nullable = True

# Calculated measurements from electrical heating or capacity settlement calculations
# Each row represents a calculated quantity for a specific metering point at a specific date.
calculated_measurements_schema = t.StructType(
    [
        #
        # "electrical_heating" or "capacity_settlement"
        t.StructField("orchestration_type", t.StringType(), not nullable),
        #
        # ID of the orchestration that initiated the calculation job
        t.StructField("orchestration_instance_id", t.StringType(), not nullable),
        #
        # Metering point ID
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # Transaction ID. Created by the calculation job.
        # The ID refers to a continous set of measurements for a specific combination of orchestration_id and metering_point_id.
        # There are no time gaps for a given transaction id. Gaps introduces a new transaction ID after the gap.
        t.StructField("transaction_id", t.StringType(), not nullable),
        #
        # A DateTime value indicating when the transaction was created
        # by the calculation job.
        t.StructField("transaction_creation_datetime", t.TimestampType(), not nullable),
        #
        # "electrical_heating" or "capacity_settlement"
        t.StructField("metering_point_type", t.StringType(), not nullable),
        #
        # UTC time
        t.StructField(
            "date",
            t.TimestampType(),
            not nullable,
        ),
        # The calculated quantity
        t.StructField("quantity", t.DecimalType(18, 3), not nullable),
    ]
)
