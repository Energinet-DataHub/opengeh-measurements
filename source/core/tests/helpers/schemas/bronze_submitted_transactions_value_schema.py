from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import (
    BronzeSubmittedTransactionsColumnNames,
    ValueColumnNames,
)

# For test purpose, this should look like our protobuf schema
bronze_submitted_transactions_value_schema = StructType(
    [
        StructField(ValueColumnNames.version, StringType(), True),
        StructField(ValueColumnNames.orchestration_instance_id, StringType(), True),
        StructField(ValueColumnNames.orchestration_type, IntegerType(), True),
        StructField(ValueColumnNames.metering_point_id, StringType(), True),
        StructField(ValueColumnNames.transaction_id, StringType(), True),
        StructField(ValueColumnNames.transaction_creation_datetime, TimestampType(), True),
        StructField(ValueColumnNames.metering_point_type, StringType(), True),
        StructField(ValueColumnNames.unit, IntegerType(), True),
        StructField(ValueColumnNames.resolution, IntegerType(), True),
        StructField(ValueColumnNames.start_datetime, TimestampType(), True),
        StructField(ValueColumnNames.end_datetime, TimestampType(), True),
        StructField(
            ValueColumnNames.points,
            ArrayType(
                StructType(
                    [
                        StructField(ValueColumnNames.Points.position, IntegerType(), True),
                        StructField(
                            ValueColumnNames.Points.quantity,
                            StructType(
                                [
                                    StructField("units", LongType(), False),
                                    StructField("nanos", IntegerType(), False),
                                ]
                            ),
                            True,
                        ),
                        StructField(ValueColumnNames.Points.quality, IntegerType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(BronzeSubmittedTransactionsColumnNames.key, BinaryType(), True),
        StructField(BronzeSubmittedTransactionsColumnNames.partition, IntegerType(), True),
    ]
)
