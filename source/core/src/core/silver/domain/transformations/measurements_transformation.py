import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DecimalType

import core.utility.datetime_helper as datetime_helper
from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import (
    ValueColumnNames,
)
from core.silver.domain.constants.col_names_silver_measurements import SilverMeasurementsColNames


def create_by_unpacked_submitted_transactions(
    spark: SparkSession, unpacked_submitted_transactions: DataFrame
) -> DataFrame:
    current_utc_time = datetime_helper.get_current_utc_timestamp(spark)

    measurements = unpacked_submitted_transactions.select(
        unpacked_submitted_transactions[ValueColumnNames.orchestration_type].alias(
            SilverMeasurementsColNames.orchestration_type
        ),
        unpacked_submitted_transactions[ValueColumnNames.orchestration_instance_id].alias(
            SilverMeasurementsColNames.orchestration_instance_id
        ),
        unpacked_submitted_transactions[ValueColumnNames.metering_point_id].alias(
            SilverMeasurementsColNames.metering_point_id
        ),
        unpacked_submitted_transactions[ValueColumnNames.transaction_id].alias(
            SilverMeasurementsColNames.transaction_id
        ),
        unpacked_submitted_transactions[ValueColumnNames.transaction_creation_datetime].alias(
            SilverMeasurementsColNames.transaction_creation_datetime
        ),
        unpacked_submitted_transactions[ValueColumnNames.metering_point_type].alias(
            SilverMeasurementsColNames.metering_point_type
        ),
        unpacked_submitted_transactions[ValueColumnNames.unit].alias(SilverMeasurementsColNames.unit),
        unpacked_submitted_transactions[ValueColumnNames.resolution].alias(SilverMeasurementsColNames.resolution),
        unpacked_submitted_transactions[ValueColumnNames.start_datetime].alias(
            SilverMeasurementsColNames.start_datetime
        ),
        unpacked_submitted_transactions[ValueColumnNames.end_datetime].alias(SilverMeasurementsColNames.end_datetime),
        F.transform(
            ValueColumnNames.points,
            lambda x: F.struct(
                x.position.alias(SilverMeasurementsColNames.Points.position),
                (x.quantity.units + (x.quantity.nanos / 1000000.0))
                .cast(DecimalType(18, 3))
                .alias(SilverMeasurementsColNames.Points.quantity),
                x.quality.alias(SilverMeasurementsColNames.Points.quality),
            ),
        ).alias(SilverMeasurementsColNames.points),
        current_utc_time.alias(SilverMeasurementsColNames.created),
    )

    return measurements
