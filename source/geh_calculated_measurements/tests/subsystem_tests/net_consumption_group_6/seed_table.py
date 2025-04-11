import random
from datetime import datetime, timezone

from geh_common.domain.types import MeteringPointType

from geh_calculated_measurements.net_consumption_group_6.infrastucture.database_definitions import (
    ElectricityMarketMeasurementsInputDatabaseDefinition,
)
from geh_calculated_measurements.testing.utilities.job_tester import JobTestFixture
from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.seed_gold_table import GoldTableRow

database = ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME
parent_table = (
    ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CONSUMPTION_METERING_POINT_PERIODS
)
child_table = ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINT

parent_metering_point_id = "170000050000000201"


def _seed_gold_table(job_fixture: JobTestFixture) -> None:
    year = datetime.now().year - 1
    gold_table_rows = [
        GoldTableRow(
            metering_point_id="170000000000000201",
            observation_time=datetime(year, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
            quantity=random.uniform(0.1, 10.0),
        ),
        GoldTableRow(
            metering_point_id="070000001500170200",
            observation_time=datetime(year, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
            quantity=random.uniform(0.1, 10.0),
            metering_point_type=MeteringPointType.CONSUMPTION_FROM_GRID,
        ),
        GoldTableRow(
            metering_point_id="060000001500170200",
            observation_time=datetime(year, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
            quantity=random.uniform(0.1, 10.0),
            metering_point_type=MeteringPointType.SUPPLY_TO_GRID,
        ),
    ]
    statement = seed_gold_table.get_statement(job_fixture.config.catalog_name, gold_table_rows)

    job_fixture.execute_statement(statement)


def delete_seeded_data(job_fixture: JobTestFixture) -> None:
    statements = []
    # PARENT
    statements.append(f"""
        DELETE FROM {job_fixture.config.catalog_name}.{database}.{parent_table} 
        WHERE metering_point_id = '{parent_metering_point_id}'
    """)
    # CHILD
    statements.append(f"""
        DELETE FROM {job_fixture.config.catalog_name}.{database}.{child_table} 
        WHERE parent_metering_point_id = '{parent_metering_point_id}'
    """)

    for statement in statements:
        job_fixture.execute_statement(statement)


def seed_electricity_market_tables(job_fixture: JobTestFixture) -> None:
    statements = []
    # PARENT
    statements.append(f"""
    INSERT INTO {job_fixture.config.catalog_name}.{database}.{parent_table} (
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
    INSERT INTO {job_fixture.config.catalog_name}.{database}.{child_table} (
        metering_point_id,
        metering_point_type,
        parent_metering_point_id,
        coupled_date,
        uncoupled_date
    )
    VALUES (
        '150000001500170200',
        'net_consumption',
        '{parent_metering_point_id}',
        '{datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc)}',
        '{datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc)}'
    )
    """)
    statements.append(f"""
    INSERT INTO {job_fixture.config.catalog_name}.{database}.{child_table} (
        metering_point_id,
        metering_point_type,
        parent_metering_point_id,
        coupled_date,
        uncoupled_date
    )
    VALUES (
        '060000001500170200',
        'supply_to_grid',
        '{parent_metering_point_id}',
        '{datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc)}',
        '{datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc)}'
    )
    """)
    statements.append(f"""
    INSERT INTO {job_fixture.config.catalog_name}.{database}.{child_table} (
        metering_point_id,
        metering_point_type,
        parent_metering_point_id,
        coupled_date,
        uncoupled_date
    )
    VALUES (
        '070000001500170200',
        'consumption_from_grid',
        '{parent_metering_point_id}',
        '{datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc)}',
        '{datetime(2025, 12, 31, 23, 0, 0, tzinfo=timezone.utc)}'
    )
    """)

    for statement in statements:
        job_fixture.execute_statement(statement)
