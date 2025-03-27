import uuid
from datetime import datetime, timezone

from geh_calculated_measurements.testing.utilities.job_tester import JobTester, JobTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.net_consumption_group_6.seed_child_table import ChildTableRow, ChildTableSeeder
from tests.subsystem_tests.net_consumption_group_6.seed_parent_table import ParentTableRow, ParentTableSeeder

job_parameters = {"orchestration-instance-id": uuid.uuid4()}

parent_table_row = ParentTableRow(
    metering_point_id="170000000000000201",
    has_electrical_heating=False,
    settlement_month=1,
    period_from_date=datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
    period_to_date=datetime(2023, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
    move_in=False,
)
child_table_row = ChildTableRow(
    metering_point_id="150000001500170200",
    metering_type="net_consumption",
    parent_metering_point_id="170000000000000201",
    coupled_date=datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
    uncoupled_date=datetime(2023, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
)


class TestNetConsumptionGroup6(JobTester):
    @property
    def fixture(self):
        config = EnvironmentConfiguration()
        parent_table_seeder = ParentTableSeeder(config)
        parent_table_seeder.seed(parent_table_row)
        child_table_seeder = ChildTableSeeder(config)
        child_table_seeder.seed(child_table_row)
        return JobTestFixture(
            environment_configuration=config,
            job_name="NetConsumptionGroup6",
            job_parameters=job_parameters,
        )
