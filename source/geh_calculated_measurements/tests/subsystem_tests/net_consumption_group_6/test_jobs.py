import random
import uuid
from datetime import datetime

import pytest
from geh_common.domain.types import MeteringPointType

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.seed_gold_table import GoldTableRow, GoldTableSeeder

fixture = None

job_parameters = {"orchestration-instance-id": uuid.uuid4()}

gold_table_row = GoldTableRow(
    metering_point_id="150000050000000201",
    observation_time=datetime(2024, 11, 30, 23, 0, 0),
    quantity=random.uniform(0.1, 10.0),
    metering_point_type=MeteringPointType.NET_CONSUMPTION,
)


@pytest.fixture(scope="session")
def setup_fixture(
    environment_configuration: EnvironmentConfiguration,
) -> BaseJobFixture:
    table_seeder = GoldTableSeeder(environment_configuration)
    table_seeder.seed(gold_table_row)
    return BaseJobFixture(
        environment_configuration=environment_configuration,
        job_name="NetConsumptionGroup6",
        job_parameters=job_parameters,
    )


job_parameters = {"orchestration-instance-id": uuid.uuid4()}


@pytest.fixture(scope="session")
def job_fixture(
    environment_configuration: EnvironmentConfiguration,
) -> BaseJobFixture:
    return BaseJobFixture(
        environment_configuration=environment_configuration,
        job_name="NetConsumptionGroup6",
        job_parameters=job_parameters,
    )


class TestNetConsumptionGroup6(BaseJobTests):
    """
    Test class for net consumption for group 6.
    """

    @pytest.mark.skip(reason="This test is temporary skipped because the storing implementation is not yet made.")
    def test__and_then_data_is_written_to_delta(
        self, environment_configuration: EnvironmentConfiguration, job_fixture: BaseJobFixture
    ) -> None:
        pass
