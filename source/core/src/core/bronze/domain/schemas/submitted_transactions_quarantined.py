from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from core.bronze.domain.constants.column_names.submitted_transactions_quarantined_column_names import (
    SubmittedTransactionsQuarantinedColumnNames,
)

submitted_transactions_quarantined_schema = StructType(
    [
        StructField(SubmittedTransactionsQuarantinedColumnNames.orchestration_type, StringType(), True),
        StructField(SubmittedTransactionsQuarantinedColumnNames.orchestration_instance_id, StringType(), True),
        StructField(SubmittedTransactionsQuarantinedColumnNames.metering_point_id, StringType(), True),
        StructField(SubmittedTransactionsQuarantinedColumnNames.transaction_id, StringType(), True),
        StructField(SubmittedTransactionsQuarantinedColumnNames.transaction_creation_datetime, TimestampType(), True),
        StructField(SubmittedTransactionsQuarantinedColumnNames.metering_point_type, StringType(), True),
        StructField(SubmittedTransactionsQuarantinedColumnNames.unit, StringType(), True),
        StructField(SubmittedTransactionsQuarantinedColumnNames.resolution, StringType(), True),
        StructField(SubmittedTransactionsQuarantinedColumnNames.start_datetime, TimestampType(), True),
        StructField(SubmittedTransactionsQuarantinedColumnNames.end_datetime, TimestampType(), True),
        StructField(
            SubmittedTransactionsQuarantinedColumnNames.points,
            ArrayType(
                StructType(
                    [
                        StructField(SubmittedTransactionsQuarantinedColumnNames.Points.position, IntegerType(), True),
                        StructField(
                            SubmittedTransactionsQuarantinedColumnNames.Points.quantity, DecimalType(18, 3), True
                        ),
                        StructField(SubmittedTransactionsQuarantinedColumnNames.Points.quality, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(SubmittedTransactionsQuarantinedColumnNames.created, TimestampType(), False),
        StructField(SubmittedTransactionsQuarantinedColumnNames.validate_orchestration_type_enum, BooleanType(), False),
        StructField(SubmittedTransactionsQuarantinedColumnNames.validate_quality_enum, BooleanType(), False),
        StructField(
            SubmittedTransactionsQuarantinedColumnNames.validate_metering_point_type_enum, BooleanType(), False
        ),
        StructField(SubmittedTransactionsQuarantinedColumnNames.validate_unit_enum, BooleanType(), False),
        StructField(SubmittedTransactionsQuarantinedColumnNames.validate_resolution_enum, BooleanType(), False),
    ]
)
