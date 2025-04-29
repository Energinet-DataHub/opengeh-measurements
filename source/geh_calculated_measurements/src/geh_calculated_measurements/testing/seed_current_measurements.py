from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from geh_common.domain.types import MeteringPointType, QuantityQuality
from pyspark.sql import SparkSession

from tests.external_data_products import ExternalDataProducts


@dataclass
class CurrentMeasurementsRow:
    metering_point_id: str
    observation_time: datetime = datetime(2025, 1, 1, 23, 0, 0, tzinfo=UTC)
    quantity: Decimal = Decimal("1.700")
    metering_point_type: MeteringPointType = MeteringPointType.CONSUMPTION
    quantity_quality: str = QuantityQuality.MEASURED.value


def seed_current_measurements(
    spark: SparkSession,
    metering_point_id: str,
    observation_time: datetime = datetime(2025, 1, 1, 23, 0, 0, tzinfo=UTC),
    quantity: Decimal = Decimal("1.700"),
    metering_point_type: MeteringPointType = MeteringPointType.CONSUMPTION,
    quantity_quality: str = QuantityQuality.MEASURED.value,
) -> None:
    seed_current_measurements_rows(
        spark,
        [
            CurrentMeasurementsRow(
                metering_point_id=metering_point_id,
                observation_time=observation_time,
                quantity=quantity,
                metering_point_type=metering_point_type,
                quantity_quality=quantity_quality,
            )
        ],
    )


def seed_current_measurements_rows(spark: SparkSession, rows: list[CurrentMeasurementsRow]) -> None:
    database_name = ExternalDataProducts.CURRENT_MEASUREMENTS.database_name
    table_name = ExternalDataProducts.CURRENT_MEASUREMENTS.view_name
    schema = ExternalDataProducts.CURRENT_MEASUREMENTS.schema

    if isinstance(rows, CurrentMeasurementsRow):
        rows = [rows]

    measurements = spark.createDataFrame(
        [
            (
                row.metering_point_id,
                row.observation_time,
                row.quantity,
                row.quantity_quality,
                row.metering_point_type.value,
            )
            for row in rows
        ],
        schema=schema,
    )

    measurements.write.saveAsTable(
        f"{database_name}.{table_name}",
        format="delta",
        mode="append",
    )
