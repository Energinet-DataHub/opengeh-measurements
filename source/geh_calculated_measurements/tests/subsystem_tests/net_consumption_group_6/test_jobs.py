import uuid
from datetime import datetime

import pytest

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.net_consumption_group_6.seed_parent_table import ParentTableRow, ParentTableSeeder

job_parameters = {"orchestration-instance-id": uuid.uuid4()}

gold_table_row = ParentTableRow(
    metering_point_id="170000000000000201",
    has_electrical_heating=False,
    settlement_month=1,
    period_from_date=datetime(2022, 12, 31, 23, 0, 0),
    period_to_date=None,  # datetime(2023, 12, 31, 23, 0, 0),
    move_in=False,
)


@pytest.fixture(scope="session")
def job_fixture(
    environment_configuration: EnvironmentConfiguration,
) -> BaseJobFixture:
    table_seeder = ParentTableSeeder(environment_configuration)
    table_seeder.seed(gold_table_row)
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

    @pytest.mark.skip(reason="This test is temporary skipped because the storing implementation is not yet made.")
    def test__and_then_data_is_written_to_delta(
        self, environment_configuration: EnvironmentConfiguration, job_fixture: BaseJobFixture
    ) -> None:
        pass
