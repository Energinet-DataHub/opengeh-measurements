from pyspark.sql import DataFrame
from pyspark.sql.protobuf.functions import from_protobuf

# This is currently a hidden import. The protobuf file is compiled to this location in the CI pipeline.
# TODO: Figure out a better solution!
descriptor_file = (
    "/source/bronze/src/opengeh_bronze/infrastructure/contracts/assets/persist_submitted_transaction.binpb"
)
message_name = "Measurement"


def transform(bronze_measurements: DataFrame) -> DataFrame:
    """Unpacks the protobuf message and maps the fields to the correct columns."""
    return bronze_measurements.transform(unpack_proto).transform(map_message)


def unpack_proto(df):
    return df.select(
        from_protobuf(df.body, message_name, descFilePath=descriptor_file).alias("measurement"), "properties"
    ).select("measurement.*", "properties")


def map_message(df):
    return df.select(
        "version",
        "orchestration_instance_id",
        "orchestratioon_type",
        "metering_point_id",
        "transaction_id",
        "transaction_creation_datetime",
        "start_datetime",
        "end_datetime",
        "metering_point_type",
        "product",
        "unit",
        "resolution",
        "points",
    )
