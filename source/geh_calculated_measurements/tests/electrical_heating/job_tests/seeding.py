from datetime import datetime, timezone

from geh_common.domain.types import MeteringPointSubType, MeteringPointType
from pyspark.sql import SparkSession

from tests.external_data_products import ExternalDataProducts


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
