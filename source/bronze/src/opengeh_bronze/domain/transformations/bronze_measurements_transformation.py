import os

from pyspark.sql import DataFrame
from pyspark.sql.protobuf.functions import from_protobuf

message_name = "PersistSubmittedTransaction"
alias_name = "measurement"


def transform(bronze_measurements: DataFrame) -> DataFrame:
    """Unpacks the protobuf message and maps the fields to the correct columns."""
    return bronze_measurements.transform(unpack_proto).transform(map_message)


def unpack_proto(df):
    # This is currently a hidden import. The protobuf file is compiled to this location in the CI pipeline.
    # TODO: Figure out a better solution!
    descriptor_file = (
        f"{os.getcwd()}/src/opengeh_bronze/infrastructure/contracts/assets/persist_submitted_transaction.binpb"
    )

    return df.select(from_protobuf(df.value, message_name, descFilePath=descriptor_file).alias(alias_name))


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
