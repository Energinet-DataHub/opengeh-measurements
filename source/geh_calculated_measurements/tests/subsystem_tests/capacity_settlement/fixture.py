from datetime import datetime, timezone

from geh_calculated_measurements.testing.utilities.job_tester import JobTestFixture
from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.seed_gold_table import GoldTableRow

database = "measurements_gold"
table = "measurements"


class Fixture(JobTestFixture):
    def __init__(self, job_parameters: dict = {}):
        config = EnvironmentConfiguration()
        super().__init__(
            environment_configuration=config,
            job_name="CapacitySettlement",
            job_parameters=job_parameters,
        )

    def seed_data(self) -> None:
        gold_table_rows = [
            GoldTableRow(
                metering_point_id="170000040000000201",
                observation_time=datetime(2025, 1, 1, 23, 0, 0, tzinfo=timezone.utc),
                quantity=i,
            )
            for i in range(10)
        ]
        statement = seed_gold_table.get_statement(self.config.catalog_name, gold_table_rows)

        self.execute_statement(statement)
