import random
from datetime import datetime, timedelta

from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobFixture
from tests.subsystem_tests.seed_gold_table import GoldTableRow

_METERING_POINT_ID = "170000060000000201"
PERIOD_START = datetime(2025, 1, 1, 23, 0, 0)
PERIOD_END = datetime(2025, 1, 2, 23, 0, 0)
_FIRST_OBSERVATION_TIME = PERIOD_START

_METERING_POINT_PERIODS_STATEMENT = f"""
        INSERT INTO electricity_market_measurements_input.missing_measurements_log_metering_point_periods_v1 (
            metering_point_id,
            grid_area_code,
            resolution,
            period_from_date,
            period_to_date
        )
        VALUES
        "('{_METERING_POINT_ID}','804','PT1H','{PERIOD_START.strftime("%Y-%m-%d %H:%M:%S")}','{PERIOD_END.strftime("%Y-%m-%d %H:%M:%S")}')"
    """


def seed_table(
    job_fixture: BaseJobFixture,
) -> None:
    gold_table_rows = [
        GoldTableRow(
            metering_point_id=_METERING_POINT_ID,
            observation_time=_FIRST_OBSERVATION_TIME + timedelta(hours=i),
            quantity=random.uniform(0.1, 10.0),
        )
        for i in range(24)
    ]
    statement = seed_gold_table.get_statement(job_fixture.environment_configuration.catalog_name, gold_table_rows)

    job_fixture.execute_statement(statement)
