from datetime import datetime, timezone
from decimal import Decimal

from geh_common.domain.types import MeteringPointSubType, MeteringPointType, QuantityQuality
from pyspark.sql import SparkSession

from tests.external_data_products import ExternalDataProducts


def seed_gold(spark: SparkSession) -> None:
    current_measurements = ExternalDataProducts.CURRENT_MEASUREMENTS

    df = spark.createDataFrame(
        [
            (  # Jan. 1st. Should be added to D14
                170000000000000201,
                datetime(2024, 1, 1, 22, 45, 0, tzinfo=timezone.utc),
                Decimal(3999.999),
                QuantityQuality.MEASURED.value,
                MeteringPointType.CONSUMPTION.value,
            ),
            (  # Jan. 2nd. Limit hit
                170000000000000201,
                datetime(2024, 1, 1, 23, 0, 0, tzinfo=timezone.utc),
                Decimal(0.001),
                QuantityQuality.MEASURED.value,
                MeteringPointType.CONSUMPTION.value,
            ),
            (  # Jan. 2nd. Even though total of 1.001 logged on parent, only 0.001 should be added to D14
                170000000000000201,
                datetime(2024, 1, 1, 23, 15, 0, tzinfo=timezone.utc),
                Decimal(1),
                QuantityQuality.MEASURED.value,
                MeteringPointType.CONSUMPTION.value,
            ),
            (  # Jan. 3rd. Nothing should be added to D14 now
                170000000000000201,
                datetime(2024, 1, 3, 15, 0, 0, tzinfo=timezone.utc),
                Decimal(500),
                QuantityQuality.MEASURED.value,
                MeteringPointType.CONSUMPTION.value,
            ),
            (  # Jan. 5th. Hit limit in one go. Only 4000 should be added to D14
                170000000000000202,
                datetime(2024, 1, 5, 17, 45, 0, tzinfo=timezone.utc),
                Decimal(4000.001),
                QuantityQuality.MEASURED.value,
                MeteringPointType.CONSUMPTION.value,
            ),
            (  # Jan. 5th. Hit limit exactly in one go
                170000000000000203,
                datetime(2024, 1, 5, 17, 45, 0, tzinfo=timezone.utc),
                Decimal(4000),
                QuantityQuality.MEASURED.value,
                MeteringPointType.CONSUMPTION.value,
            ),
            (  # Jan. 6th.  Nothing should be added to D14 now
                170000000000000203,
                datetime(2024, 1, 6, 10, 30, 0, tzinfo=timezone.utc),
                Decimal(0.001),
                QuantityQuality.MEASURED.value,
                MeteringPointType.CONSUMPTION.value,
            ),
        ],
        schema=current_measurements.schema,
    )

    df.write.saveAsTable(
        f"{current_measurements.database_name}.{current_measurements.view_name}",
        format="delta",
        mode="append",
    )


def seed_electricity_market(spark: SparkSession) -> None:
    # Consumption
    consumption_metering_point_periods = ExternalDataProducts.ELECTRICAL_HEATING_CONSUMPTION_METERING_POINT_PERIODS
    df = spark.createDataFrame(
        [
            (170000000000000201, 2, 1, datetime(2024, 12, 31, 23, 0, 0, tzinfo=timezone.utc), None),
            (170000000000000202, 2, 1, datetime(2023, 12, 31, 23, 0, 0, tzinfo=timezone.utc), None),
            (170000000000000203, 2, 1, datetime(2023, 12, 31, 23, 0, 0, tzinfo=timezone.utc), None),
        ],
        schema=consumption_metering_point_periods.schema,
    )
    df.write.format("delta").mode("append").saveAsTable(
        f"{consumption_metering_point_periods.database_name}.{consumption_metering_point_periods.view_name}"
    )

    # Child
    child_metering_points = ExternalDataProducts.ELECTRICAL_HEATING_CHILD_METERING_POINTS
    df = spark.createDataFrame(
        [
            (
                140000000000170201,
                MeteringPointType.ELECTRICAL_HEATING.value,
                MeteringPointSubType.CALCULATED.value,
                170000000000000201,
                datetime(2023, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                None,
            ),
            (
                140000000000170202,
                MeteringPointType.ELECTRICAL_HEATING.value,
                MeteringPointSubType.CALCULATED.value,
                170000000000000202,
                datetime(2023, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                None,
            ),
            (
                140000000000170203,
                MeteringPointType.ELECTRICAL_HEATING.value,
                MeteringPointSubType.CALCULATED.value,
                170000000000000203,
                datetime(2023, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                None,
            ),
        ],
        schema=child_metering_points.schema,
    )

    df.write.format("delta").mode("append").saveAsTable(
        f"{child_metering_points.database_name}.{child_metering_points.view_name}"
    )
