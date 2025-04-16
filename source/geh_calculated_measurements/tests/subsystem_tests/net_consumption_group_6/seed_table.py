import random
from datetime import datetime, timezone

from geh_common.data_products.electricity_market_measurements_input import (
    net_consumption_group_6_child_metering_points_v1,
    net_consumption_group_6_consumption_metering_point_periods_v1,
)

from geh_calculated_measurements.testing.utilities.job_tester import JobTestFixture
from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.seed_gold_table import GoldTableRow

parent_table = net_consumption_group_6_consumption_metering_point_periods_v1.view_name
child_table = net_consumption_group_6_child_metering_points_v1.view_name

parent_metering_point_id = "170000050000000201"


# TODO JMK: Do not prefix with _ in symbols used outside the module
def _seed_gold_table(job_fixture: JobTestFixture) -> None:
    gold_table_rows = [
        GoldTableRow(
            metering_point_id="170000000000000201",
            observation_time=datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
            quantity=random.uniform(0.1, 10.0),
        )
        for i in range(1)
    ]
    statement = seed_gold_table.get_statement(job_fixture.config.catalog_name, gold_table_rows)

    job_fixture.execute_statement(statement)


def delete_seeded_data(job_fixture: JobTestFixture) -> None:
    statements = []
    # PARENT
    statements.append(f"""
        DELETE FROM {job_fixture.config.catalog_name}.{job_fixture.config.electricity_market_database_name}.{parent_table} 
        WHERE metering_point_id = '{parent_metering_point_id}'
    """)
    # CHILD
    statements.append(f"""
        DELETE FROM {job_fixture.config.catalog_name}.{job_fixture.config.electricity_market_database_name}.{child_table} 
        WHERE parent_metering_point_id = '{parent_metering_point_id}'
    """)

    for statement in statements:
        job_fixture.execute_statement(statement)


def seed_electricity_market_tables(job_fixture: JobTestFixture) -> None:
    statements = []
    # PARENT
    statements.append(f"""
    INSERT INTO {job_fixture.config.catalog_name}.{job_fixture.config.electricity_market_database_name}.{parent_table} (
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
    INSERT INTO {job_fixture.config.catalog_name}.{job_fixture.config.electricity_market_database_name}.{child_table} (
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
    INSERT INTO {job_fixture.config.catalog_name}.{job_fixture.config.electricity_market_database_name}.{child_table} (
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
    INSERT INTO {job_fixture.config.catalog_name}.{job_fixture.config.electricity_market_database_name}.{child_table} (
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
