import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.protobuf.functions import to_protobuf

from core.contracts.process_manager.Brs021ForwardMeteredDataNotifyV1.brs021_forward_metered_data_notify_v1_column_names import (
    Brs021ForwardMeteredDataNotifyV1ColumnNames,
    Brs021ForwardMeteredDataNotifyV1EventColumnNames,
)
from core.contracts.process_manager.descriptor_paths import DescriptorFilePaths
from core.contracts.process_manager.PersistSubmittedTransaction.persist_submitted_transaction_proto_version import (
    PersistSubmittedTransactionProtoVersion,
)
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames

alias_name = "measurement_values"


def transform(submitted_transactions: DataFrame) -> DataFrame:
    return submitted_transactions.transform(prepare_measurement).transform(pack_proto)


def prepare_measurement(df) -> DataFrame:
    return df.select(
        F.struct(
            F.lit(PersistSubmittedTransactionProtoVersion.version_1).alias(
                Brs021ForwardMeteredDataNotifyV1ColumnNames.version
            ),
            df[SilverMeasurementsColumnNames.orchestration_instance_id].alias(
                Brs021ForwardMeteredDataNotifyV1ColumnNames.orchestration_instance_id
            ),
        ).alias(Brs021ForwardMeteredDataNotifyV1EventColumnNames.value)
    )


def pack_proto(df) -> DataFrame:
    message_name = "Brs021ForwardMeteredDataNotifyV1"
    return df.withColumn(
        Brs021ForwardMeteredDataNotifyV1EventColumnNames.value,
        to_protobuf(
            Brs021ForwardMeteredDataNotifyV1EventColumnNames.value,
            message_name,
            descFilePath=DescriptorFilePaths.Brs021ForwardMeteredDataNotifyV1,
        ),
    )
