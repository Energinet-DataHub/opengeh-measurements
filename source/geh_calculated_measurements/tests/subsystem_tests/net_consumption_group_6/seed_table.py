import random
from datetime import datetime, timezone

from geh_common.domain.types import MeteringPointType, OrchestrationType

from geh_calculated_measurements.testing import JobTestFixture
from tests import CalculationType, create_random_metering_point_id
from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.seed_gold_table import GoldTableRow

parent_table = "net_consumption_group_6_consumption_metering_point_periods"
child_table = "net_consumption_group_6_child_metering_point"

parent_metering_point_id = create_random_metering_point_id(CalculationType.NET_CONSUMPTION)
net_consumption_metering_point_id = create_random_metering_point_id(CalculationType.NET_CONSUMPTION)
consumption_from_grid_metering_point_id = create_random_metering_point_id(CalculationType.NET_CONSUMPTION)
supply_to_grid_metering_point_id = create_random_metering_point_id(CalculationType.NET_CONSUMPTION)


def seed_table(
    job_fixture: JobTestFixture,
) -> None:
    catalog_name = job_fixture.config.catalog_name
    database_name = job_fixture.config.electricity_market_internal_database_name
    job_fixture.execute_statement(gold_table_statement(catalog_name))
    for statement in electricity_market_tables_statements(catalog_name, database_name):
        job_fixture.execute_statement(statement)


def gold_table_statement(catalog_name: str) -> str:
    year = datetime.now().year - 1
    gold_table_rows = [
        GoldTableRow(
            metering_point_id=net_consumption_metering_point_id,
            metering_point_type=MeteringPointType.NET_CONSUMPTION,
            orchestration_type=OrchestrationType.NET_CONSUMPTION,
            observation_time=datetime(year, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
            quantity=random.uniform(0.1, 10.0),
        ),
        GoldTableRow(
            metering_point_id=consumption_from_grid_metering_point_id,
            metering_point_type=MeteringPointType.CONSUMPTION_FROM_GRID,
            orchestration_type=OrchestrationType.NET_CONSUMPTION,
            observation_time=datetime(year, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
            quantity=random.uniform(0.1, 10.0),
        ),
        GoldTableRow(
            metering_point_id=supply_to_grid_metering_point_id,
            metering_point_type=MeteringPointType.SUPPLY_TO_GRID,
            orchestration_type=OrchestrationType.NET_CONSUMPTION,
            observation_time=datetime(year, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
            quantity=random.uniform(0.1, 10.0),
        ),
    ]
    return seed_gold_table.get_statement(catalog_name, gold_table_rows)


def electricity_market_tables_statements(catalog_name: str, database_name: str) -> list[str]:
    statements = []
    # PARENT
    statements.append(f"""
    INSERT INTO {catalog_name}.{database_name}.{parent_table} (
        metering_point_id,
        has_electrical_heating,
        settlement_month,
        period_from_date,
        period_to_date,
        move_in
    )
    VALUES (
        '{parent_metering_point_id}',
        {False},
        {1},
        '{datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}',
        '{datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}',
        {False}
    )
    """)
    # CHILDREN
    statements.append(f"""
    INSERT INTO {catalog_name}.{database_name}.{child_table} (
        metering_point_id,
        metering_point_type,
        parent_metering_point_id,
        coupled_date,
        uncoupled_date
    )
    VALUES (
        '{net_consumption_metering_point_id}',
        '{MeteringPointType.NET_CONSUMPTION.value}',
        '{parent_metering_point_id}',
        '{datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc)}',
        '{datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc)}'
    )
    """)
    statements.append(f"""
    INSERT INTO {catalog_name}.{database_name}.{child_table} (
        metering_point_id,
        metering_point_type,
        parent_metering_point_id,
        coupled_date,
        uncoupled_date
    )
    VALUES (
        '{supply_to_grid_metering_point_id}',
        '{MeteringPointType.SUPPLY_TO_GRID.value}',
        '{parent_metering_point_id}',
        '{datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc)}',
        '{datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc)}'
    )
    """)
    statements.append(f"""
    INSERT INTO {catalog_name}.{database_name}.{child_table} (
        metering_point_id,
        metering_point_type,
        parent_metering_point_id,
        coupled_date,
        uncoupled_date
    )
    VALUES (
        '{consumption_from_grid_metering_point_id}',
        '{MeteringPointType.CONSUMPTION_FROM_GRID.value}',
        '{parent_metering_point_id}',
        '{datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc)}',
        '{datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc)}'
    )
    """)
    return statements


def delete_seeded_data(job_fixture: JobTestFixture) -> None:
    statements = []
    # PARENT
    statements.append(f"""
        DELETE FROM {job_fixture.config.catalog_name}.{job_fixture.config.electricity_market_internal_database_name}.{parent_table}
        WHERE metering_point_id = '{parent_metering_point_id}'
    """)
    # CHILD
    statements.append(f"""
        DELETE FROM {job_fixture.config.catalog_name}.{job_fixture.config.electricity_market_internal_database_name}.{child_table}
        WHERE parent_metering_point_id = '{parent_metering_point_id}'
    """)

    for statement in statements:
        job_fixture.execute_statement(statement)
