import random
from datetime import datetime, timedelta, timezone

from geh_calculated_measurements.testing.utilities.job_tester import JobTestFixture
from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.seed_gold_table import GoldTableRow

_METERING_POINT_ID = "170000060000000201"
PERIOD_START = datetime(2025, 1, 1, 23, 0, 0, tzinfo=timezone.utc)
PERIOD_END = datetime(2025, 1, 2, 23, 0, 0, tzinfo=timezone.utc)


def get_metering_point_periods_statement(catalog_name: str) -> str:
    return f"""
        INSERT INTO {catalog_name}.electricity_market_measurements_input.missing_measurements_log_metering_point_periods_v1 (
            metering_point_id,
            grid_area_code,
            resolution,
            period_from_date,
            period_to_date
        )
        VALUES
        ('{_METERING_POINT_ID}','804','PT1H','{PERIOD_START.strftime("%Y-%m-%d %H:%M:%S")}','{PERIOD_END.strftime("%Y-%m-%d %H:%M:%S")}')
    """


def gold_table_statement(catalog_name: str) -> str:
    gold_table_rows = [
        GoldTableRow(
            metering_point_id=_METERING_POINT_ID,
            observation_time=PERIOD_START + timedelta(hours=i),
            quantity=random.uniform(0.1, 10.0),
        )
        for i in range(24)
    ]
    return seed_gold_table.get_statement(catalog_name, gold_table_rows)


def seed_table(
    job_fixture: JobTestFixture,
) -> None:
    catalog_name = job_fixture.config.catalog_name
    job_fixture.execute_statement(gold_table_statement(catalog_name))

    # TODO JMG: Data cannot be modified through views
    # job_fixture.execute_statement(get_metering_point_periods_statement(catalog_name))
