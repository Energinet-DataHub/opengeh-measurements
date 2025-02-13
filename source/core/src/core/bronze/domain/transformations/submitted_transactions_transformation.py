from pyspark.sql import DataFrame
from pyspark.sql.protobuf.functions import from_protobuf

from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import (
    BronzeSubmittedTransactionsColumnNames,
    ValueColumnNames,
)
from core.bronze.domain.constants.descriptor_file_names import DescriptorFileNames
from core.bronze.infrastructure.helpers.path_helper import get_protobuf_descriptor_path

alias_name = "measurement_values"


def created_by_packed_submitted_transactions(submitted_transactions: DataFrame) -> DataFrame:
    """Unpacks the protobuf message and maps the fields to the correct columns."""
    return submitted_transactions.transform(_unpack_proto).transform(_map_submitted_transactions)


def _unpack_proto(df) -> DataFrame:
    descriptor_path = get_protobuf_descriptor_path(DescriptorFileNames.persist_submitted_transaction)
    message_name = "PersistSubmittedTransaction"
    return df.select(
        from_protobuf(df.value, message_name, descFilePath=descriptor_path).alias(alias_name),
        BronzeSubmittedTransactionsColumnNames.key,
        BronzeSubmittedTransactionsColumnNames.partition,
    )


def _map_submitted_transactions(df) -> DataFrame:
    return df.select(
        f"{alias_name}.{ValueColumnNames.version}",
        f"{alias_name}.{ValueColumnNames.orchestration_instance_id}",
        f"{alias_name}.{ValueColumnNames.orchestration_type}",
        f"{alias_name}.{ValueColumnNames.metering_point_id}",
        f"{alias_name}.{ValueColumnNames.transaction_id}",
        f"{alias_name}.{ValueColumnNames.transaction_creation_datetime}",
        f"{alias_name}.{ValueColumnNames.metering_point_type}",
        f"{alias_name}.{ValueColumnNames.unit}",
        f"{alias_name}.{ValueColumnNames.resolution}",
        f"{alias_name}.{ValueColumnNames.start_datetime}",
        f"{alias_name}.{ValueColumnNames.end_datetime}",
        f"{alias_name}.{ValueColumnNames.points}",
        BronzeSubmittedTransactionsColumnNames.key,
        BronzeSubmittedTransactionsColumnNames.partition,
    )
