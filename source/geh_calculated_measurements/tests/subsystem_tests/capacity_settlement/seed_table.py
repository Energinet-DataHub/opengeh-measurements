import random
from datetime import datetime, timezone

from geh_common.domain.types import MeteringPointType, OrchestrationType

from geh_calculated_measurements.testing import JobTestFixture
from tests import CalculationType, create_random_metering_point_id
from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.seed_gold_table import GoldTableRow

PARENT_METERING_POINT_ID = create_random_metering_point_id(CalculationType.CAPACITY_SETTLEMENT)
CHILD_METERING_POINT_ID = create_random_metering_point_id(CalculationType.CAPACITY_SETTLEMENT)


def seed_table(job_fixture: JobTestFixture) -> None:
    gold_table_rows = [
        GoldTableRow(
            metering_point_id=PARENT_METERING_POINT_ID,
            metering_point_type=MeteringPointType.CAPACITY_SETTLEMENT,
            orchestration_type=OrchestrationType.CAPACITY_SETTLEMENT,
            observation_time=datetime(2025, 1, 1, 23, 0, 0, tzinfo=timezone.utc),
            quantity=random.uniform(0.1, 10.0),
        )
    ]
    statement = seed_gold_table.get_statement(job_fixture.config.catalog_name, gold_table_rows)

    job_fixture.execute_statement(statement)


def seed_electricity_market(job_fixture: JobTestFixture) -> None:
    catalog_name = job_fixture.config.catalog_name
    database_name = job_fixture.config.electricity_market_internal_database_name
    capacity_settlement_table = "capacity_settlement_metering_point_periods"

    statement = f"""INSERT INTO {catalog_name}.{database_name}.{capacity_settlement_table} (
        metering_point_id,
        period_from_date,
        period_to_date,
        child_metering_point_id,
        child_period_from_date,
        child_period_to_date
    )
    VALUES (
        '{PARENT_METERING_POINT_ID}',
        '{datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}',
        NULL,
        '{CHILD_METERING_POINT_ID}',
        '{datetime(2024, 12, 31, 23, 0, 0, tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}',
        NULL
    )"""
    job_fixture.execute_statement(statement)
