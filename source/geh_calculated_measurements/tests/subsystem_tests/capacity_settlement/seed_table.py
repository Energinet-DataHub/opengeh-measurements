from datetime import datetime

from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.seed_gold_table import GoldTableRow

database = "measurements_gold"
table = "measurements"


def seed_table(
    job_fixture,
) -> None:
    gold_table_rows = [
        GoldTableRow(
            metering_point_id=170000060000000201,
            observation_time=datetime(2025, 1, 1, 23, 0, 0),
            quantity=i,
        )
        for i in range(10)
    ]
    statement = seed_gold_table.get_statement(job_fixture.environment_configuration.catalog_name, gold_table_rows)

    job_fixture.execute_statement(statement)
