import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.protobuf.functions import to_protobuf

from core.contracts.process_manager.persist_submitted_transaction_proto_version import (
    PersistSubmittedTransactionProtoVersion,
)
from core.contracts.process_manager.submitted_transactions_column_names import (
    SubmittedTransactionEventColumnNames,
    SubmittedTransactionsColumnNames,
)
from core.silver.domain.constants.col_names_silver_measurements import SilverMeasurementsColNames
from core.silver.domain.constants.descriptor_file_names import DescriptorFileNames
from core.utility.path_helper import get_protobuf_descriptor_path

alias_name = "measurement_values"


def transform(submitted_transactions: DataFrame) -> DataFrame:
    return submitted_transactions.transform(prepare_measurement).transform(pack_proto)


def prepare_measurement(df) -> DataFrame:
    return df.select(
        F.struct(
            F.lit(PersistSubmittedTransactionProtoVersion.version).alias(SubmittedTransactionsColumnNames.version),
            df[SilverMeasurementsColNames.orchestration_instance_id].alias(
                SubmittedTransactionsColumnNames.orchestration_instance_id
            ),
            df[SilverMeasurementsColNames.orchestration_type].alias(
                SubmittedTransactionsColumnNames.orchestration_type
            ),
        ).alias(SubmittedTransactionEventColumnNames.value)
    )


def pack_proto(df) -> DataFrame:
    descriptor_path = get_protobuf_descriptor_path(DescriptorFileNames.submitted_transaction_persisted)
    message_name = "SubmittedTransactionPersisted"
    return df.withColumn(
        SubmittedTransactionEventColumnNames.value,
        to_protobuf(SubmittedTransactionEventColumnNames.value, message_name, descFilePath=descriptor_path),
    )
