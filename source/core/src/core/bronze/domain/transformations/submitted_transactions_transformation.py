from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf

from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import (
    BronzeSubmittedTransactionsColumnNames,
    ValueColumnNames,
)
from core.bronze.domain.constants.descriptor_file_names import DescriptorFileNames
from core.bronze.infrastructure.helpers.path_helper import get_protobuf_descriptor_path

alias_name = "measurement_values"


def create_by_packed_submitted_transactions(submitted_transactions: DataFrame) -> DataFrame:
    """Unpacks the protobuf message and maps the fields to the correct columns."""
    return submitted_transactions.transform(_unpack_proto).transform(_map_submitted_transactions)


def _unpack_proto(df) -> DataFrame:
    descriptor_path = get_protobuf_descriptor_path(DescriptorFileNames.persist_submitted_transaction)
    message_name = "PersistSubmittedTransaction"

    options = {"mode": "PERMISSIVE", "recursive.fields.max.depth": "3", "emit.default.values": "true"}

    unpacked = df.select(
        from_protobuf(df.value, message_name, descFilePath=descriptor_path, options=options).alias(alias_name),
        BronzeSubmittedTransactionsColumnNames.key,
        BronzeSubmittedTransactionsColumnNames.partition,
    )

    return unpacked


def _map_submitted_transactions(df) -> DataFrame:
    return df.select(
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
