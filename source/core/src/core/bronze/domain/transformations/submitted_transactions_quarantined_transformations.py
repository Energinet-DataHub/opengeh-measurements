from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp

from core.bronze.domain.constants.column_names.submitted_transactions_quarantined_column_names import (
    SubmittedTransactionsQuarantinedColumnNames,
)
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames


def map_silver_measurements_to_submitted_transactions_quarantined(silver_measurements: DataFrame) -> DataFrame:
    return silver_measurements.select(
        col(SilverMeasurementsColumnNames.orchestration_type).alias(
            SubmittedTransactionsQuarantinedColumnNames.orchestration_type
        ),
        col(SilverMeasurementsColumnNames.orchestration_instance_id).alias(
            SubmittedTransactionsQuarantinedColumnNames.orchestration_instance_id
        ),
        col(SilverMeasurementsColumnNames.metering_point_id).alias(
            SubmittedTransactionsQuarantinedColumnNames.metering_point_id
        ),
        col(SilverMeasurementsColumnNames.transaction_id).alias(
            SubmittedTransactionsQuarantinedColumnNames.transaction_id
        ),
        col(SilverMeasurementsColumnNames.transaction_creation_datetime).alias(
            SubmittedTransactionsQuarantinedColumnNames.transaction_creation_datetime
        ),
        col(SilverMeasurementsColumnNames.metering_point_type).alias(
            SubmittedTransactionsQuarantinedColumnNames.metering_point_type
        ),
        col(SilverMeasurementsColumnNames.unit).alias(SubmittedTransactionsQuarantinedColumnNames.unit),
        col(SilverMeasurementsColumnNames.resolution).alias(SubmittedTransactionsQuarantinedColumnNames.resolution),
        col(SilverMeasurementsColumnNames.start_datetime).alias(
            SubmittedTransactionsQuarantinedColumnNames.start_datetime
        ),
        col(SilverMeasurementsColumnNames.end_datetime).alias(SubmittedTransactionsQuarantinedColumnNames.end_datetime),
        col(SilverMeasurementsColumnNames.points).alias(SubmittedTransactionsQuarantinedColumnNames.points),
        current_timestamp().alias(SubmittedTransactionsQuarantinedColumnNames.created),
        col(SubmittedTransactionsQuarantinedColumnNames.validate_orchestration_type_enum),
        col(SubmittedTransactionsQuarantinedColumnNames.validate_quality_enum),
        col(SubmittedTransactionsQuarantinedColumnNames.validate_metering_point_type_enum),
        col(SubmittedTransactionsQuarantinedColumnNames.validate_unit_enum),
        col(SubmittedTransactionsQuarantinedColumnNames.validate_resolution_enum),
    )
