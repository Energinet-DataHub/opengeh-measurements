import pyspark.sql.functions as F
from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from pyspark.sql import Column, DataFrame

import core.silver.infrastructure.config.spark_session as spark_session
import core.utility.datetime_helper as datetime_helper
from core.bronze.domain.constants.column_names.bronze_migrated_transactions_column_names import (
    BronzeMigratedTransactionsColumnNames,
    BronzeMigratedTransactionsValuesFieldNames,
)
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames
from core.silver.domain.constants.enums.read_reason_enum import ReadReasonEnum
from core.silver.domain.constants.enums.status_enum import StatusEnum

MIGRATION_ORCHESTRATION_INSTANCE_ID = "00000000-0000-0000-0000-000000000000"


def transform(migrated_transactions: DataFrame) -> DataFrame:
    spark = spark_session.initialize_spark()
    current_utc_time = datetime_helper.get_current_utc_timestamp(spark)

    measurements = migrated_transactions.select(
        F.lit(GehCommonOrchestrationType.MIGRATION.value).alias(SilverMeasurementsColumnNames.orchestration_type),
        F.lit(MIGRATION_ORCHESTRATION_INSTANCE_ID).alias(SilverMeasurementsColumnNames.orchestration_instance_id),
        F.col(BronzeMigratedTransactionsColumnNames.metering_point_id).alias(
            SilverMeasurementsColumnNames.metering_point_id
        ),
        F.col(BronzeMigratedTransactionsColumnNames.transaction_id).alias(SilverMeasurementsColumnNames.transaction_id),
        F.col(BronzeMigratedTransactionsColumnNames.transaction_insert_date).alias(
            SilverMeasurementsColumnNames.transaction_creation_datetime
        ),
        F.col(BronzeMigratedTransactionsColumnNames.type_of_mp).alias(
            SilverMeasurementsColumnNames.metering_point_type
        ),
        F.col(BronzeMigratedTransactionsColumnNames.unit).alias(SilverMeasurementsColumnNames.unit),
        F.col(BronzeMigratedTransactionsColumnNames.resolution).alias(SilverMeasurementsColumnNames.resolution),
        F.col(BronzeMigratedTransactionsColumnNames.valid_from_date).alias(
            SilverMeasurementsColumnNames.start_datetime
        ),
        F.col(BronzeMigratedTransactionsColumnNames.valid_to_date).alias(SilverMeasurementsColumnNames.end_datetime),
        _reorganize_values_array_to_match_measurements().alias(SilverMeasurementsColumnNames.points),
        _get_is_cancelled().alias(SilverMeasurementsColumnNames.is_cancelled),
        current_utc_time.alias(SilverMeasurementsColumnNames.created),
    )
    return measurements


def _reorganize_values_array_to_match_measurements() -> Column:
    return F.transform(
        F.col(BronzeMigratedTransactionsColumnNames.values),
        lambda x: F.struct(
            x[BronzeMigratedTransactionsValuesFieldNames.position],
            x[BronzeMigratedTransactionsValuesFieldNames.quantity]
            .cast("Decimal(18,3)")
            .alias(BronzeMigratedTransactionsValuesFieldNames.quantity),
            x[BronzeMigratedTransactionsValuesFieldNames.quality],
        ),
    )


def _get_is_cancelled() -> Column:
    return (F.col(BronzeMigratedTransactionsColumnNames.read_reason) == ReadReasonEnum.CAN.value) | (
        F.col(BronzeMigratedTransactionsColumnNames.status) == StatusEnum.Deleted.value
    )
