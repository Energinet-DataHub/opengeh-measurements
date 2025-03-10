import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession

import core.utility.datetime_helper as datetime_helper
from src.core.bronze.domain.constants.column_names.bronze_migrated_transactions_column_names import (
    BronzeMigratedTransactionsColumnNames
)
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames
from core.silver.domain.constants.enums.read_reason_enum import ReadReasonEnum 
from core.silver.domain.constants.enums.status_enum import StatusEnum

def create_by_migrated_transactions(
    spark: SparkSession, migrated_transactions: DataFrame
) -> DataFrame:
    current_utc_time = datetime_helper.get_current_utc_timestamp(spark)

    measurements = migrated_transactions.select(
        F.lit("migration").alias(SilverMeasurementsColumnNames.orchestration_type),     
        F.lit("00000000-0000-0000-0000-000000000000").alias(SilverMeasurementsColumnNames.orchestration_instance_id),
        F.col(BronzeMigratedTransactionsColumnNames.metering_point_id).alias(SilverMeasurementsColumnNames.metering_point_id),
        F.col(BronzeMigratedTransactionsColumnNames.transaction_id).alias(SilverMeasurementsColumnNames.transaction_id),
        F.col(BronzeMigratedTransactionsColumnNames.transaction_insert_date).alias(SilverMeasurementsColumnNames.transaction_creation_datetime),
        F.col(BronzeMigratedTransactionsColumnNames.type_of_mp).alias(SilverMeasurementsColumnNames.metering_point_type),
        F.col(BronzeMigratedTransactionsColumnNames.unit).alias(SilverMeasurementsColumnNames.unit),
        F.col(BronzeMigratedTransactionsColumnNames.resolution).alias(SilverMeasurementsColumnNames.resolution),
        F.col(BronzeMigratedTransactionsColumnNames.valid_from_date).alias(SilverMeasurementsColumnNames.start_datetime),
        F.col(BronzeMigratedTransactionsColumnNames.valid_to_date).alias(SilverMeasurementsColumnNames.end_datetime),
        F.col(BronzeMigratedTransactionsColumnNames.values).alias(SilverMeasurementsColumnNames.points), 
        _get_is_cancelled().alias(SilverMeasurementsColumnNames.is_cancelled),
        _get_is_deleted().alias(SilverMeasurementsColumnNames.is_deleted),      
        current_utc_time.alias(SilverMeasurementsColumnNames.created),
    )

    return measurements


def _get_is_cancelled() -> Column:
    return F.col(BronzeMigratedTransactionsColumnNames.read_reason) == ReadReasonEnum.CAN.value


def _get_is_deleted() -> Column:
    return F.col(BronzeMigratedTransactionsColumnNames.status) == StatusEnum.Deleted.value

