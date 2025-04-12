import random
from datetime import datetime, timedelta, timezone

from geh_common.data_products.electricity_market_measurements_input import (
    missing_measurements_log_metering_point_periods_v1,
)
from geh_common.domain.types import MeteringPointResolution

from geh_calculated_measurements.testing.utilities.job_tester import JobTestFixture
from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.seed_gold_table import GoldTableRow

_GRID_AREA_CODE = "804"
_METERING_POINT_ID = "170000060000000201"
PERIOD_FROM_DATE = datetime(2025, 1, 1, 23, 0, 0, tzinfo=timezone.utc)
PERIOD_TO_DATE = datetime(2025, 1, 2, 23, 0, 0, tzinfo=timezone.utc)


def _get_metering_point_periods_statement(catalog_name: str) -> str:
    return f"""
        INSERT INTO {catalog_name}.{missing_measurements_log_metering_point_periods_v1.database_name}.{missing_measurements_log_metering_point_periods_v1.view_name} (
            metering_point_id,
            grid_area_code,
            resolution,
            period_from_date,
            period_to_date
        )
        VALUES
        ('{_METERING_POINT_ID}',
         '{_GRID_AREA_CODE}',
         '{MeteringPointResolution.HOUR.value}',
         '{PERIOD_FROM_DATE.strftime("%Y-%m-%d %H:%M:%S")}',
         '{PERIOD_TO_DATE.strftime("%Y-%m-%d %H:%M:%S")}')
    """


def _create_gold_table_statement(catalog_name: str) -> str:
    gold_table_rows = [
        GoldTableRow(
            metering_point_id=_METERING_POINT_ID,
            observation_time=PERIOD_FROM_DATE + timedelta(hours=i),
            quantity=random.uniform(0.1, 10.0),
        )
        for i in range(24)
    ]
    return seed_gold_table.get_statement(catalog_name, gold_table_rows)


def seed_data_products(
    job_fixture: JobTestFixture,
) -> None:
    # Seed the measurements core gold table
    catalog_name = job_fixture.config.catalog_name
    job_fixture.execute_statement(_create_gold_table_statement(catalog_name))

    # TODO: JMG - a view cannot be seeded
    # job_fixture.execute_statement(_get_metering_point_periods_statement(catalog_name))
