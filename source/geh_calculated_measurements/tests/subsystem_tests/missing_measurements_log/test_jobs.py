import uuid

import pytest

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

fixture = None

job_parameters = {"orchestration-instance-id": uuid.uuid4()}


@pytest.fixture(scope="session")
def setup_fixture(
    environment_configuration: EnvironmentConfiguration,
) -> BaseJobFixture:
    return BaseJobFixture(
        environment_configuration=environment_configuration,
        job_name="MissingMeasurementsLog",
        job_parameters=job_parameters,
    )


class TestMissingMeasurementsLog(BaseJobTests):
    """
    Test class for missing measurements log.
    """

    def test__and_then_data_is_written_to_delta(
        self, environment_configuration: EnvironmentConfiguration, setup_fixture: BaseJobFixture
    ) -> None:
        pass
