from pyspark.sql.types import (
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
)

submitted_transactions_schema = StructType(
    [
        StructField(BronzeSubmittedTransactionsColumnNames.key, BinaryType(), True),
        StructField(BronzeSubmittedTransactionsColumnNames.value, BinaryType(), True),
        StructField(BronzeSubmittedTransactionsColumnNames.topic, StringType(), True),
        StructField(BronzeSubmittedTransactionsColumnNames.partition, IntegerType(), True),
        StructField(BronzeSubmittedTransactionsColumnNames.offset, LongType(), True),
        StructField(BronzeSubmittedTransactionsColumnNames.timestamp, TimestampType(), True),
        StructField(BronzeSubmittedTransactionsColumnNames.timestamp_type, IntegerType(), True),
    ]
)
