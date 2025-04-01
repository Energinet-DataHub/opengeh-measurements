import pyspark.sql.types as t

nullable = True


hourly_calculated_measurements_v1 = t.StructType(
    [
        # "electrical_heating" or "capacity_settlement"
        t.StructField("orchestration_type", t.StringType(), not nullable),
        #
        # ID of the orchestration that initiated the calculation job
        t.StructField("orchestration_instance_id", t.StringType(), not nullable),
        #
        # Transaction ID created by the calculation job. The ID refers to a continous set of measurements for a
        # specific combination of orchestration_id and metering_point_id. There are no time gaps for a given
        # transaction id. Gaps introduces a new transaction ID after the gap.
        t.StructField("transaction_id", t.StringType(), not nullable),
        #
        # A datetime value indicating when the transaction was created by the calculation job.
        t.StructField("transaction_creation_datetime", t.TimestampType(), not nullable),
        #
        # GSRN number
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # 'electrical_heating' | 'capacity_settlement'
        t.StructField("metering_point_type", t.StringType(), not nullable),
        #
        # UTC datetime
        t.StructField("observation_time", t.TimestampType(), not nullable),
        #
        # The calculated quantity
        t.StructField("quantity", t.DecimalType(18, 3), not nullable),
        #
        # The unit of the calculated quantity
        # "kWh"
        t.StructField("quantity_unit", t.StringType(), not nullable),
        #
        # The quality of the calculated quantity
        # "calculated"
        t.StructField("quantity_quality", t.StringType(), not nullable),
        #
        # The resolution of the calculated quantity
        # "PT1H"
        t.StructField("resolution", t.StringType(), not nullable),
    ]
)
