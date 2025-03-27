import random
from datetime import datetime

from geh_calculated_measurements.testing.utilities.job_tester import JobTestFixture
from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.seed_gold_table import GoldTableRow

database = "measurements_gold"
table = "measurements"


def seed_table(job_fixture: JobTestFixture) -> None:
    gold_table_rows = [
        GoldTableRow(
            metering_point_id=170000060000000201,
            observation_time=datetime(2024, 11, 30, 23, 0, 0),
            quantity=random.uniform(0.1, 10.0),
        )
        for i in range(1)
    ]
    statement = seed_gold_table.get_statement(job_fixture.environment_configuration.catalog_name, gold_table_rows)

    job_fixture.execute_statement(statement)
