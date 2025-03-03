import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DecimalType

import core.utility.datetime_helper as datetime_helper
from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import (
    ValueColumnNames,
)
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames


def create_by_unpacked_submitted_transactions(
    spark: SparkSession, unpacked_submitted_transactions: DataFrame
) -> DataFrame:
    current_utc_time = datetime_helper.get_current_utc_timestamp(spark)

    measurements = unpacked_submitted_transactions.select(
        unpacked_submitted_transactions[ValueColumnNames.orchestration_type].alias(
            SilverMeasurementsColumnNames.orchestration_type
        ),
        unpacked_submitted_transactions[ValueColumnNames.orchestration_instance_id].alias(
            SilverMeasurementsColumnNames.orchestration_instance_id
        ),
        unpacked_submitted_transactions[ValueColumnNames.metering_point_id].alias(
            SilverMeasurementsColumnNames.metering_point_id
        ),
        unpacked_submitted_transactions[ValueColumnNames.transaction_id].alias(
            SilverMeasurementsColumnNames.transaction_id
        ),
        unpacked_submitted_transactions[ValueColumnNames.transaction_creation_datetime].alias(
            SilverMeasurementsColumnNames.transaction_creation_datetime
        ),
        unpacked_submitted_transactions[ValueColumnNames.metering_point_type].alias(
            SilverMeasurementsColumnNames.metering_point_type
        ),
        unpacked_submitted_transactions[ValueColumnNames.unit].alias(SilverMeasurementsColumnNames.unit),
        unpacked_submitted_transactions[ValueColumnNames.resolution].alias(SilverMeasurementsColumnNames.resolution),
        unpacked_submitted_transactions[ValueColumnNames.start_datetime].alias(
            SilverMeasurementsColumnNames.start_datetime
        ),
        unpacked_submitted_transactions[ValueColumnNames.end_datetime].alias(
            SilverMeasurementsColumnNames.end_datetime
        ),
        F.transform(
            ValueColumnNames.points,
            lambda x: F.struct(
                x.position.alias(SilverMeasurementsColumnNames.Points.position),
                (x.quantity.units + (x.quantity.nanos / 1000000.0))
                .cast(DecimalType(18, 3))
                .alias(SilverMeasurementsColumnNames.Points.quantity),
                x.quality.alias(SilverMeasurementsColumnNames.Points.quality),
            ),
        ).alias(SilverMeasurementsColumnNames.points),
        F.lit(False).alias(SilverMeasurementsColumnNames.is_cancelled),
        F.lit(False).alias(SilverMeasurementsColumnNames.is_deleted),
        current_utc_time.alias(SilverMeasurementsColumnNames.created),
    )

    return measurements
