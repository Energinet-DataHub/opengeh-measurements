from pyspark.sql import DataFrame
from pyspark.sql.protobuf.functions import from_protobuf

import opengeh_bronze.infrastructure.helpers.path_helper as path_helper

message_name = "PersistSubmittedTransaction"
alias_name = "measurement"


def transform(bronze_measurements: DataFrame) -> DataFrame:
    """Unpacks the protobuf message and maps the fields to the correct columns."""
    return bronze_measurements.transform(unpack_proto).transform(map_message)


def unpack_proto(df):
    descriptor_path = path_helper.get_protobuf_descriptor_path("persist_submitted_transaction.binpb")
    return df.select(from_protobuf(df.value, message_name, descFilePath=descriptor_path).alias(alias_name))


def map_message(df):
    return df.select(
        f"{alias_name}.version",
        f"{alias_name}.orchestration_instance_id",
        f"{alias_name}.orchestration_type",
        f"{alias_name}.metering_point_id",
        f"{alias_name}.transaction_id",
        f"{alias_name}.transaction_creation_datetime",
        f"{alias_name}.start_datetime",
        f"{alias_name}.end_datetime",
        f"{alias_name}.metering_point_type",
        f"{alias_name}.product",
        f"{alias_name}.unit",
        f"{alias_name}.resolution",
        f"{alias_name}.points",
    )
