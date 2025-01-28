import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from opengeh_bronze.domain.constants.column_names.bronze_measurements_column_names import BronzeMeasurementsColumnNames
from opengeh_bronze.domain.constants.column_names.notify_transactions_persisted_event_column_names import (
    NotifyTransactionsPersistedEventColumnNames,
)


def transform(bronze_measurements: DataFrame) -> DataFrame:
    notify_transactions_persisted_event = bronze_measurements.select(
        F.col(BronzeMeasurementsColumnNames.orchestration_instance_id).alias(
            NotifyTransactionsPersistedEventColumnNames.key
        ),
    )

    return notify_transactions_persisted_event
