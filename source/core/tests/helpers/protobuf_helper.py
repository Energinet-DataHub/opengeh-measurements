from importlib.resources import files

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.protobuf.functions import from_protobuf

from core.contracts.process_manager.brs021_forward_metered_data_notify_v1_column_names import (
    Brs021ForwardMeteredDataNotifyV1ColumnNames,
    Brs021ForwardMeteredDataNotifyV1EventColumnNames,
)
from core.silver.domain.constants.descriptor_file_names import DescriptorFileNames

alias_name = "measurement_values"


def unpack_brs021_forward_metered_data_notify_v1(brs021_forward_metered_data_notify_v1: DataFrame) -> DataFrame:
    descriptor_path = str(
        files("core.contracts.process_manager.assets").joinpath(
            DescriptorFileNames.brs021_forward_metered_data_notify_v1
        )
    )
    message_name = "Brs021ForwardMeteredDataNotifyV1"

    options = {"recursive.fields.max.depth": "3", "emit.default.values": "true"}

    unpacked = brs021_forward_metered_data_notify_v1.select(
        from_protobuf(
            Brs021ForwardMeteredDataNotifyV1EventColumnNames.value,
            message_name,
            descFilePath=descriptor_path,
            options=options,
        ).alias(alias_name),
        Brs021ForwardMeteredDataNotifyV1EventColumnNames.value,
    )

    return unpacked.select(
        F.col(f"{alias_name}.{Brs021ForwardMeteredDataNotifyV1ColumnNames.version}").alias(
            Brs021ForwardMeteredDataNotifyV1ColumnNames.version
        ),
        F.col(f"{alias_name}.{Brs021ForwardMeteredDataNotifyV1ColumnNames.orchestration_instance_id}").alias(
            Brs021ForwardMeteredDataNotifyV1ColumnNames.orchestration_instance_id
        ),
    )
