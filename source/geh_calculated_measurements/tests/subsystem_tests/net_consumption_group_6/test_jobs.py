import uuid
from datetime import datetime

import pytest

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.net_consumption_group_6.seed_child_table import ChildTableRow, ChildTableSeeder
from tests.subsystem_tests.net_consumption_group_6.seed_parent_table import ParentTableRow, ParentTableSeeder

job_parameters = {"orchestration-instance-id": uuid.uuid4()}

parent_table_row = ParentTableRow(
    metering_point_id="170000000000000201",
    has_electrical_heating=False,
    settlement_month=1,
    period_from_date=datetime(2022, 12, 31, 23, 0, 0),
    period_to_date=datetime(2023, 12, 31, 23, 0, 0),
    move_in=False,
)
child_table_row = ChildTableRow(
    metering_point_id="150000001500170200",
    metering_type="net_consumption",
    parent_metering_point_id="170000000000000201",
    coupled_date=datetime(2022, 12, 31, 23, 0, 0),
    uncoupled_date=datetime(2023, 12, 31, 23, 0, 0),
)


@pytest.fixture(scope="session")
def job_fixture(
    environment_configuration: EnvironmentConfiguration,
) -> BaseJobFixture:
    parent_table_seeder = ParentTableSeeder(environment_configuration)
    parent_table_seeder.seed(parent_table_row)
    child_table_seeder = ChildTableSeeder(environment_configuration)
    child_table_seeder.seed(child_table_row)
    return BaseJobFixture(
        environment_configuration=environment_configuration,
        job_name="NetConsumptionGroup6",
        job_parameters=job_parameters,
    )


class TestNetConsumptionGroup6(BaseJobTests):
    """
    Test class for net consumption for group 6.
    """

    @pytest.mark.skip(reason="Skipped due to issues with the telemetry data not available in the logs.")
    def test__and_then_job_telemetry_is_created(self, job_fixture: BaseJobFixture) -> None:
        pass
