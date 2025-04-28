from datetime import datetime, timezone

from geh_common.domain.types import MeteringPointType, OrchestrationType

from geh_calculated_measurements.testing.utilities.job_tester import JobTestFixture
from tests import CalculationType, create_random_metering_point_id
from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.seed_gold_table import GoldTableRow


def _seed_gold_table(job_fixture: JobTestFixture, metering_point_id: str) -> None:
    gold_table_rows = [
        GoldTableRow(
            metering_point_id=metering_point_id,
            metering_point_type=MeteringPointType.CAPACITY_SETTLEMENT,
            orchestration_type=OrchestrationType.CAPACITY_SETTLEMENT,
            observation_time=datetime(2025, 1, 1, 23, 0, 0, tzinfo=timezone.utc),
            quantity=i,
        )
        for i in range(10)
    ]
    statement = seed_gold_table.get_statement(job_fixture.config.catalog_name, gold_table_rows)

    job_fixture.execute_statement(statement)


def _seed_metering_point_periods_table(job_fixture: JobTestFixture, metering_point_id: str) -> None:
    catalog_name = job_fixture.config.catalog_name
    database_name = job_fixture.config.electricity_market_internal_database_name
    job_fixture.execute_statement(
        f"""
        INSERT INTO {catalog_name}.{database_name}.missing_measurements_log_metering_point_periods (
            metering_point_id,
            grid_area_code,
            resolution,
            period_from_date,
            period_to_date
        )
        VALUES (
            '{metering_point_id}',
            '804',
            'HOUR',
            '2025-01-01 23:00:00',
            '2025-01-03 23:00:00'
        )
    """
    )


def seed_tables(job_fixture: JobTestFixture) -> None:
    metering_point_id = create_random_metering_point_id(CalculationType.CAPACITY_SETTLEMENT)
    _seed_gold_table(job_fixture, metering_point_id)
