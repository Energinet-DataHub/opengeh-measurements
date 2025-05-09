import random
from datetime import datetime, timedelta, timezone

from geh_common.domain.types import MeteringPointResolution, MeteringPointType, OrchestrationType

from geh_calculated_measurements.testing import JobTestFixture
from tests import CalculationType, create_random_metering_point_id
from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.seed_gold_table import GoldTableRow

missing_measurements_log_metering_point_periods_table_name = "missing_measurements_log_metering_point_periods"

_METERING_POINT_ID = create_random_metering_point_id(CalculationType.MISSING_MEASUREMENTS_LOG)
_GRID_AREA_CODE = "804"
PERIOD_START = datetime(2025, 1, 1, 23, 0, 0, tzinfo=timezone.utc)
PERIOD_END = datetime(2025, 1, 3, 23, 0, 0, tzinfo=timezone.utc)


def seed_table(
    job_fixture: JobTestFixture,
) -> None:
    catalog_name = job_fixture.config.catalog_name
    database_name = job_fixture.config.electricity_market_internal_database_name
    job_fixture.execute_statement(gold_table_statement(catalog_name))
    job_fixture.execute_statement(get_metering_point_periods_statement(catalog_name, database_name))


def get_metering_point_periods_statement(catalog_name: str, database_name: str) -> str:
    return f"""
        INSERT INTO {catalog_name}.{database_name}.{missing_measurements_log_metering_point_periods_table_name} (
            metering_point_id,
            grid_area_code,
            resolution,
            period_from_date,
            period_to_date
        )
        VALUES (
            '{_METERING_POINT_ID}',
            '{_GRID_AREA_CODE}',
            '{MeteringPointResolution.HOUR.value}',
            '{PERIOD_START.strftime("%Y-%m-%d %H:%M:%S")}',
            '{PERIOD_END.strftime("%Y-%m-%d %H:%M:%S")}'
        )
    """


def gold_table_statement(catalog_name: str) -> str:
    gold_table_rows = [
        GoldTableRow(
            metering_point_id=_METERING_POINT_ID,
            metering_point_type=MeteringPointType.CAPACITY_SETTLEMENT,
            orchestration_type=OrchestrationType.CAPACITY_SETTLEMENT,
            observation_time=PERIOD_START + timedelta(hours=i),
            quantity=random.uniform(0.1, 10.0),
        )
        for i in range(24)
    ]
    return seed_gold_table.get_statement(catalog_name, gold_table_rows)


def delete_seeded_data(job_fixture: JobTestFixture) -> None:
    statement = f"""
        DELETE FROM {job_fixture.config.catalog_name}.{job_fixture.config.electricity_market_internal_database_name}.{missing_measurements_log_metering_point_periods_table_name}
        WHERE metering_point_id = '{_METERING_POINT_ID}'
    """

    job_fixture.execute_statement(statement)
