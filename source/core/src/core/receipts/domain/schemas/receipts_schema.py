from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from core.receipts.domain.constants.column_names.process_manager_receipts_column_names import (
    ProcessManagerReceiptsColumnNames,
)

receipts_schema = StructType(
    [
        StructField(ProcessManagerReceiptsColumnNames.orchestration_instance_id, StringType(), True),
        StructField(ProcessManagerReceiptsColumnNames.created, TimestampType(), True),
    ]
)
