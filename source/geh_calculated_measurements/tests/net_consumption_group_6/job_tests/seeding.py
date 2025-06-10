from datetime import datetime, timezone
from decimal import Decimal

from geh_common.domain.types import MeteringPointType, OrchestrationType, QuantityQuality

from geh_calculated_measurements.common.application.model import CalculatedMeasurementsInternal
from geh_calculated_measurements.common.domain.column_names import ContractColumnNames
from tests.conftest import ExternalDataProducts
from tests.internal_tables import InternalTables


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
            {
                ContractColumnNames.metering_point_id: parent_metering_point_id,
                ContractColumnNames.observation_time: datetime(2024, 1, 31, 23, 0, 0, tzinfo=timezone.utc),
                ContractColumnNames.quantity: Decimal(2000),
                ContractColumnNames.quality: QuantityQuality.MEASURED.value,
                ContractColumnNames.metering_point_type: MeteringPointType.CONSUMPTION.value,
            },
            {
                ContractColumnNames.metering_point_id: child_supply_to_grid_metering_point,
                ContractColumnNames.observation_time: datetime(2024, 1, 31, 23, 0, 0, tzinfo=timezone.utc),
                ContractColumnNames.quantity: Decimal(1000),
                ContractColumnNames.quality: QuantityQuality.MEASURED.value,
                ContractColumnNames.metering_point_type: MeteringPointType.SUPPLY_TO_GRID.value,
            },
            {
                ContractColumnNames.metering_point_id: child_consumption_from_grid_metering_point,
                ContractColumnNames.observation_time: datetime(2024, 1, 31, 23, 0, 0, tzinfo=timezone.utc),
                ContractColumnNames.quantity: Decimal(2000),
                ContractColumnNames.quality: QuantityQuality.ESTIMATED.value,
                ContractColumnNames.metering_point_type: MeteringPointType.CONSUMPTION_FROM_GRID.value,
            },
            {
                ContractColumnNames.metering_point_id: child_net_consumption_metering_point,
                ContractColumnNames.observation_time: datetime(2024, 12, 30, 23, 0, 0, tzinfo=timezone.utc),
                ContractColumnNames.quantity: Decimal(3),
                ContractColumnNames.quality: QuantityQuality.CALCULATED.value,
                ContractColumnNames.metering_point_type: MeteringPointType.NET_CONSUMPTION.value,
            },
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
            {
                ContractColumnNames.metering_point_id: parent_metering_point_id,
                ContractColumnNames.has_electrical_heating: False,
                ContractColumnNames.settlement_month: 1,
                ContractColumnNames.period_from_date: datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                ContractColumnNames.period_to_date: None,
                ContractColumnNames.move_in: False,
            }
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
            {
                ContractColumnNames.metering_point_id: child_net_consumption_metering_point,
                ContractColumnNames.metering_point_type: MeteringPointType.NET_CONSUMPTION.value,
                ContractColumnNames.parent_metering_point_id: parent_metering_point_id,
                ContractColumnNames.coupled_date: datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                ContractColumnNames.uncoupled_date: None,
            },
            {
                ContractColumnNames.metering_point_id: child_supply_to_grid_metering_point,
                ContractColumnNames.metering_point_type: MeteringPointType.SUPPLY_TO_GRID.value,
                ContractColumnNames.parent_metering_point_id: parent_metering_point_id,
                ContractColumnNames.coupled_date: datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                ContractColumnNames.uncoupled_date: None,
            },
            {
                ContractColumnNames.metering_point_id: child_consumption_from_grid_metering_point,
                ContractColumnNames.metering_point_type: MeteringPointType.CONSUMPTION_FROM_GRID.value,
                ContractColumnNames.parent_metering_point_id: parent_metering_point_id,
                ContractColumnNames.coupled_date: datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                ContractColumnNames.uncoupled_date: None,
            },
        ],
        schema=child_metering_points.schema,
    )

    df.write.format("delta").mode("append").saveAsTable(
        f"{child_metering_points.database_name}.{child_metering_points.view_name}"
    )


def _seed_internal(
    spark,
    child_net_consumption_metering_point: str,
) -> None:
    calculated_measurements = InternalTables.CALCULATED_MEASUREMENTS

    df = spark.createDataFrame(
        [
            {
                ContractColumnNames.orchestration_type: OrchestrationType.NET_CONSUMPTION.value,
                ContractColumnNames.orchestration_instance_id: "1",
                ContractColumnNames.transaction_id: "1",
                ContractColumnNames.transaction_creation_datetime: datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                ContractColumnNames.transaction_start_time: datetime(2024, 12, 30, 23, 0, 0, tzinfo=timezone.utc),
                ContractColumnNames.transaction_end_time: datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                ContractColumnNames.metering_point_id: child_net_consumption_metering_point,
                ContractColumnNames.metering_point_type: MeteringPointType.NET_CONSUMPTION.value,
                ContractColumnNames.observation_time: datetime(2024, 12, 30, 23, 0, 0, tzinfo=timezone.utc),
                ContractColumnNames.quantity: Decimal("3"),
                ContractColumnNames.settlement_type: "up_to_end_of_period",
            }
        ],
        schema=CalculatedMeasurementsInternal.schema,
    )

    # Persist the data to the table
    df.write.saveAsTable(
        f"{calculated_measurements.database_name}.{calculated_measurements.table_name}",
        format="delta",
        mode="append",
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
    _seed_internal(
        spark,
        child_net_consumption_metering_point,
    )
