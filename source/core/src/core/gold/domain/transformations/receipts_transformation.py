import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from core.gold.domain.constants.column_names.gold_measurements_column_names import GoldMeasurementsColumnNames
from core.receipts.domain.constants.column_names.process_manager_receipts_column_names import (
    ProcessManagerReceiptsColumnNames,
)


def transform(df: DataFrame) -> DataFrame:
    return df.select(
        F.col(GoldMeasurementsColumnNames.orchestration_instance_id).alias(
            ProcessManagerReceiptsColumnNames.orchestration_instance_id
        ),
        F.current_timestamp().alias(ProcessManagerReceiptsColumnNames.created),
    )
