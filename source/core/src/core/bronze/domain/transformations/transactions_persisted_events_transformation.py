import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.protobuf.functions import to_protobuf

from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import (
    BronzeSubmittedTransactionsColumnNames,
    ValueColumnNames,
)
from core.bronze.domain.constants.descriptor_file_names import DescriptorFileNames
from core.bronze.infrastructure.helpers.path_helper import get_protobuf_descriptor_path


def create_by_unpacked_submitted_transactions(unpacked_submitted_transactions: DataFrame) -> DataFrame:
    return unpacked_submitted_transactions.transform(_prepare_measurement).transform(_pack_proto)


def _prepare_measurement(df) -> DataFrame:
    return df.select(
        BronzeSubmittedTransactionsColumnNames.key,
        F.struct(
            df[ValueColumnNames.version].alias(ValueColumnNames.version),
            df[ValueColumnNames.orchestration_instance_id].alias(ValueColumnNames.orchestration_instance_id),
            df[ValueColumnNames.orchestration_type].alias(ValueColumnNames.orchestration_type),
        ).alias(BronzeSubmittedTransactionsColumnNames.value),
        BronzeSubmittedTransactionsColumnNames.partition,
    )


def _pack_proto(df) -> DataFrame:
    descriptor_path = get_protobuf_descriptor_path(DescriptorFileNames.submitted_transaction_persisted)
    message_name = "SubmittedTransactionPersisted"
    return df.withColumn(
        BronzeSubmittedTransactionsColumnNames.value,
        to_protobuf(BronzeSubmittedTransactionsColumnNames.value, message_name, descFilePath=descriptor_path),
    )
