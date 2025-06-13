from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf

from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import (
    BronzeSubmittedTransactionsColumnNames,
    ValueColumnNames,
)
from core.contracts.process_manager.descriptor_paths import DescriptorFilePaths

alias_name = "measurement_values"


def unpack(submitted_transactions) -> tuple[DataFrame, DataFrame]:
    """Return a tuple with the unpacked submitted transactions and the invalid ones."""
    message_name = "PersistSubmittedTransaction"

    options = {
        "mode": "PERMISSIVE",
        "recursive.fields.max.depth": "3",
        "emit.default.values": "true",
        "enums.as.ints": "true",
    }

    unpacked = submitted_transactions.select(
        from_protobuf(
            submitted_transactions.value,
            message_name,
            descFilePath=DescriptorFilePaths.PersistSubmittedTransactionV2,
            options=options,
        ).alias(alias_name),
        BronzeSubmittedTransactionsColumnNames.key,
        BronzeSubmittedTransactionsColumnNames.partition,
        BronzeSubmittedTransactionsColumnNames.offset,
        BronzeSubmittedTransactionsColumnNames.value,
        BronzeSubmittedTransactionsColumnNames.topic,
        BronzeSubmittedTransactionsColumnNames.timestamp,
        BronzeSubmittedTransactionsColumnNames.timestamp_type,
    )

    valid_submitted_transactions = _get_valid_submitted_transactions(unpacked)
    invalid_submitted_transactions = _get_invalid_submitted_transactions(unpacked)

    return (valid_submitted_transactions, invalid_submitted_transactions)


def _get_valid_submitted_transactions(submitted_transactions: DataFrame) -> DataFrame:
    valid_submitted_transactions = submitted_transactions.filter(F.col(alias_name).isNotNull())

    return valid_submitted_transactions.select(
        F.col(f"{alias_name}.{ValueColumnNames.version}").alias(ValueColumnNames.version),
        F.col(f"{alias_name}.{ValueColumnNames.orchestration_instance_id}").alias(
            ValueColumnNames.orchestration_instance_id
        ),
        F.col(f"{alias_name}.{ValueColumnNames.orchestration_type}").alias(ValueColumnNames.orchestration_type),
        F.col(f"{alias_name}.{ValueColumnNames.metering_point_id}").alias(ValueColumnNames.metering_point_id),
        F.col(f"{alias_name}.{ValueColumnNames.transaction_id}").alias(ValueColumnNames.transaction_id),
        F.col(f"{alias_name}.{ValueColumnNames.transaction_creation_datetime}").alias(
            ValueColumnNames.transaction_creation_datetime
        ),
        F.col(f"{alias_name}.{ValueColumnNames.metering_point_type}").alias(ValueColumnNames.metering_point_type),
        F.col(f"{alias_name}.{ValueColumnNames.unit}").alias(ValueColumnNames.unit),
        F.col(f"{alias_name}.{ValueColumnNames.resolution}").alias(ValueColumnNames.resolution),
        F.col(f"{alias_name}.{ValueColumnNames.start_datetime}").alias(ValueColumnNames.start_datetime),
        F.col(f"{alias_name}.{ValueColumnNames.end_datetime}").alias(ValueColumnNames.end_datetime),
        F.col(f"{alias_name}.{ValueColumnNames.points}").alias(ValueColumnNames.points),
        BronzeSubmittedTransactionsColumnNames.key,
        BronzeSubmittedTransactionsColumnNames.partition,
    )


def _get_invalid_submitted_transactions(submitted_transactions) -> DataFrame:
    invalid_submitted_transactions = submitted_transactions.filter(F.col(alias_name).isNull())

    return invalid_submitted_transactions.select(
        BronzeSubmittedTransactionsColumnNames.key,
        BronzeSubmittedTransactionsColumnNames.partition,
        BronzeSubmittedTransactionsColumnNames.offset,
        BronzeSubmittedTransactionsColumnNames.value,
        BronzeSubmittedTransactionsColumnNames.topic,
        BronzeSubmittedTransactionsColumnNames.timestamp,
        BronzeSubmittedTransactionsColumnNames.timestamp_type,
    )
