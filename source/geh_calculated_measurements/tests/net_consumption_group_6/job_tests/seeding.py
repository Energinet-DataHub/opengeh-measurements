from datetime import datetime, timezone
from decimal import Decimal

from geh_common.domain.types import MeteringPointType, QuantityQuality

from tests.conftest import ExternalDataProducts


def _seed_gold(
    spark,
    parent_metering_point_id: str,
    child_consumption_from_grid_metering_point: str,
    child_net_consumption_metering_point: str,
    child_supply_to_grid_metering_point: str,
) -> None:
    current_measurements = ExternalDataProducts.CURRENT_MEASUREMENTS

    df = spark.createDataFrame(
        [
            (
                parent_metering_point_id,
                datetime(2024, 1, 31, 23, 0, 0, tzinfo=timezone.utc),
                Decimal(2000),
                QuantityQuality.MEASURED.value,
                MeteringPointType.CONSUMPTION.value,
            ),
            (
                child_supply_to_grid_metering_point,
                datetime(2024, 1, 31, 23, 0, 0, tzinfo=timezone.utc),
                Decimal(1000),
                QuantityQuality.MEASURED.value,
                MeteringPointType.SUPPLY_TO_GRID.value,
            ),
            (
                child_consumption_from_grid_metering_point,
                datetime(2024, 1, 31, 23, 0, 0, tzinfo=timezone.utc),
                Decimal(2000),
                QuantityQuality.ESTIMATED.value,
                MeteringPointType.CONSUMPTION_FROM_GRID.value,
            ),
            (
                child_net_consumption_metering_point,
                datetime(2024, 12, 30, 23, 0, 0, tzinfo=timezone.utc),
                Decimal(3),
                QuantityQuality.CALCULATED.value,
                MeteringPointType.NET_CONSUMPTION.value,
            ),
        ],
        schema=current_measurements.schema,
    )

    # Persist the data to the table
    df.write.saveAsTable(
        f"{current_measurements.database_name}.{current_measurements.view_name}",
        format="delta",
        mode="append",
    )


def _seed_electricity_market(
    spark,
    parent_metering_point_id: str,
    child_consumption_from_grid_metering_point: str,
    child_net_consumption_metering_point: str,
    child_supply_to_grid_metering_point: str,
) -> None:
    # CONSUMPTION
    consumption_metering_point_periods = ExternalDataProducts.NET_CONSUMPTION_GROUP_6_CONSUMPTION_METERING_POINT_PERIODS
    df = spark.createDataFrame(
        [
            (
                parent_metering_point_id,
                False,
                1,
                datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                False,
            )
        ],
        schema=consumption_metering_point_periods.schema,
    )

    df.write.format("delta").mode("append").saveAsTable(
        f"{consumption_metering_point_periods.database_name}.{consumption_metering_point_periods.view_name}"
    )

    # CHILD
    child_metering_points = ExternalDataProducts.NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINTS
    df = spark.createDataFrame(
        [
            (
                child_net_consumption_metering_point,
                MeteringPointType.NET_CONSUMPTION.value,
                parent_metering_point_id,
                datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                None,
            ),
            (
                child_supply_to_grid_metering_point,
                MeteringPointType.SUPPLY_TO_GRID.value,
                parent_metering_point_id,
                datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                None,
            ),
            (
                child_consumption_from_grid_metering_point,
                MeteringPointType.CONSUMPTION_FROM_GRID.value,
                parent_metering_point_id,
                datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                None,
            ),
        ],
        schema=child_metering_points.schema,
    )
    df.write.format("delta").mode("append").saveAsTable(
        f"{child_metering_points.database_name}.{child_metering_points.view_name}"
    )


def seed(
    spark,
    parent_metering_point_id: str,
    child_consumption_from_grid_metering_point: str,
    child_net_consumption_metering_point: str,
    child_supply_to_grid_metering_point: str,
) -> None:
    _seed_gold(
        spark,
        parent_metering_point_id,
        child_consumption_from_grid_metering_point,
        child_net_consumption_metering_point,
        child_supply_to_grid_metering_point,
    )
    _seed_electricity_market(
        spark,
        parent_metering_point_id,
        child_consumption_from_grid_metering_point,
        child_net_consumption_metering_point,
        child_supply_to_grid_metering_point,
    )
