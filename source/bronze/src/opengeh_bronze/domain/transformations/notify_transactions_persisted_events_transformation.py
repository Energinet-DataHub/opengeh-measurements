import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.protobuf.functions import to_protobuf

from opengeh_bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import (
    BronzeSubmittedTransactionsColumnNames,
    ValueColumnNames,
)

# This is currently a hidden import. The protobuf file is compiled to this location in the CI pipeline.
# TODO: Figure out a better solution!
descriptor_file = (
    "/source/bronze/src/opengeh_bronze/infrastructure/contracts/assets/submitted_transaction_persisted.binpb"
)
message_name = "Measurement"


def transform(unpacked_bronze_measurements: DataFrame) -> DataFrame:
    return unpacked_bronze_measurements.transform(prepare_measurement).transform(pack_proto)


def prepare_measurement(df):
    return df.withColumn(
        BronzeSubmittedTransactionsColumnNames.value,
        F.struct(
            df[ValueColumnNames.version].alias(ValueColumnNames.version),
            df[ValueColumnNames.orchestration_instance_id].alias(ValueColumnNames.orchestration_instance_id),
            df[ValueColumnNames.orchestration_type].alias(ValueColumnNames.orchestration_type),
        ),
    )


def pack_proto(df):
    return df.withColumn("value", to_protobuf(df.value, message_name, descFilePath=descriptor_file))
