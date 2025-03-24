import uuid

import pytest

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

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

    @pytest.mark.skip(reason="Skipped due to issues with the telemetry data not available in the logs.")
    def test__and_then_job_telemetry_is_created(self, job_fixture: BaseJobFixture) -> None:
        pass

    @pytest.mark.skip(reason="This test is temporary skipped because the storing implementation is not yet made.")
    def test__and_then_data_is_written_to_delta(
        self, environment_configuration: EnvironmentConfiguration, job_fixture: BaseJobFixture
    ) -> None:
        pass
