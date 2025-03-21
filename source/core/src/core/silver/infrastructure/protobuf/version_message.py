from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf

from core.contracts.process_manager.descriptor_paths import DescriptorFilePaths


def with_version(protobuf_message: DataFrame) -> DataFrame:
    message_name = "VersionMessage"

    protobuf_message = protobuf_message.withColumn(
        "version_message",
        from_protobuf(
            protobuf_message.value,
            message_name,
            descFilePath=DescriptorFilePaths.VersionMessage,
        ),
    )

    protobuf_message = protobuf_message.select("*", F.col("version_message.version").alias("version"))

    return protobuf_message
