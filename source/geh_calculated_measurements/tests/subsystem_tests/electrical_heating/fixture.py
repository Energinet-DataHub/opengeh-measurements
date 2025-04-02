import random
from datetime import datetime, timezone

from geh_calculated_measurements.testing.utilities.job_tester import JobTestFixture
from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.seed_gold_table import GoldTableRow

database = "measurements_gold"
table = "measurements"


class Fixture(JobTestFixture):
    """
    Fixture class for Electrical Heating job tests.
    """

    def __init__(self, job_parameters: dict = {}):
        config = EnvironmentConfiguration()
        super().__init__(
            environment_configuration=config,
            job_name="ElectricalHeating",
            job_parameters=job_parameters,
        )

    def seed_data(self) -> None:
        gold_table_rows = [
            GoldTableRow(
                metering_point_id="170000030000000201",
                observation_time=datetime(2024, 11, 30, 23, 0, 0, tzinfo=timezone.utc),
                quantity=random.uniform(0.1, 10.0),
            )
            for i in range(1)
        ]
        statement = seed_gold_table.get_statement(self.config.catalog_name, gold_table_rows)

        self.execute_statement(statement)
