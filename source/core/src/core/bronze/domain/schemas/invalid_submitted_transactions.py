from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from core.bronze.domain.constants.column_names.bronze_invalid_submitted_transactions_column_names import (
    BronzeInvalidSubmittedTransactionsColumnNames,
)

invalid_submitted_transactions_schema = StructType(
    [
        StructField(BronzeInvalidSubmittedTransactionsColumnNames.key, BinaryType(), True),
        StructField(BronzeInvalidSubmittedTransactionsColumnNames.value, BinaryType(), True),
        StructField(BronzeInvalidSubmittedTransactionsColumnNames.topic, StringType(), True),
        StructField(BronzeInvalidSubmittedTransactionsColumnNames.partition, IntegerType(), True),
        StructField(BronzeInvalidSubmittedTransactionsColumnNames.offset, LongType(), True),
        StructField(BronzeInvalidSubmittedTransactionsColumnNames.timestamp, TimestampType(), True),
        StructField(BronzeInvalidSubmittedTransactionsColumnNames.timestamp_type, IntegerType(), True),
        StructField(BronzeInvalidSubmittedTransactionsColumnNames.version, StringType(), True),
    ]
)
