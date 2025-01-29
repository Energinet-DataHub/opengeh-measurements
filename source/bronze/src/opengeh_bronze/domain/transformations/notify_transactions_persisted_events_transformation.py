import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.protobuf.functions import to_protobuf

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
        "value",
        F.struct(
            df["orchestration_instance_id"].alias("orchestration_instance_id"),
            df["orchestration_type"].alias("orchestration_type"),
            df["metering_point_id"].alias("metering_point_id"),
            df["transaction_id"].alias("transaction_id"),
            df["transaction_creation_datetime"].alias("transaction_creation_datetime"),
            df["start_datetime"].alias("start_datetime"),
            df["end_datetime"].alias("end_datetime"),
            df["metering_point_type"].alias("metering_point_type"),
            df["product"].alias("product"),
            df["unit"].alias("unit"),
            df["resolution"].alias("resolution"),
            df["points"].alias("points"),
        ),
    )


def pack_proto(df):
    return df.withColumn("body", to_protobuf(df.value, message_name, descFilePath=descriptor_file))
