from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    StructField,
    StructType,
)

from core.bronze.domain.constants.column_names.transactions_persisted_event_column_names import (
    TransactionsPersistedEventColumnNames,
)

transactions_persisted_event = StructType(
    [
        StructField(TransactionsPersistedEventColumnNames.key, BinaryType(), True),
        StructField(TransactionsPersistedEventColumnNames.value, BinaryType(), True),
        StructField(TransactionsPersistedEventColumnNames.partition, IntegerType(), True),
    ]
)
