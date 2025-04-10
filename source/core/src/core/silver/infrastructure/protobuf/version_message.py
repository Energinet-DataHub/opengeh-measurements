from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf

from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import ValueColumnNames
from core.contracts.process_manager.descriptor_paths import DescriptorFilePaths


def with_version(protobuf_message: DataFrame) -> DataFrame:
    message_name = "VersionMessage"
    message_alias = "version_message"

    options = {"mode": "PERMISSIVE"}

    protobuf_message = protobuf_message.withColumn(
        message_alias,
        from_protobuf(
            protobuf_message.value, message_name, descFilePath=DescriptorFilePaths.VersionMessage, options=options
        ),
    )

    protobuf_message = protobuf_message.select(
        "*", F.col(f"{message_alias}.{ValueColumnNames.version}").alias(ValueColumnNames.version)
    ).drop(message_alias)

    return protobuf_message
