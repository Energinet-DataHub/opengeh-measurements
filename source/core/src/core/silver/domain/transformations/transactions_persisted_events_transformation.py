from importlib.resources import files

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
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames
from core.silver.domain.constants.descriptor_file_names import DescriptorFileNames

alias_name = "measurement_values"


def transform(submitted_transactions: DataFrame) -> DataFrame:
    return submitted_transactions.transform(prepare_measurement).transform(pack_proto)


def prepare_measurement(df) -> DataFrame:
    return df.select(
        F.struct(
            F.lit(PersistSubmittedTransactionProtoVersion.version_1).alias(SubmittedTransactionsColumnNames.version),
            df[SilverMeasurementsColumnNames.orchestration_instance_id].alias(
                SubmittedTransactionsColumnNames.orchestration_instance_id
            ),
            df[SilverMeasurementsColumnNames.orchestration_type].alias(
                SubmittedTransactionsColumnNames.orchestration_type
            ),
        ).alias(SubmittedTransactionEventColumnNames.value)
    )


def pack_proto(df) -> DataFrame:
    descriptor_path = str(
        files("core.contracts.process_manager.assets").joinpath(DescriptorFileNames.submitted_transaction_persisted)
    )
    message_name = "SubmittedTransactionPersisted"
    return df.withColumn(
        SubmittedTransactionEventColumnNames.value,
        to_protobuf(SubmittedTransactionEventColumnNames.value, message_name, descFilePath=descriptor_path),
    )
