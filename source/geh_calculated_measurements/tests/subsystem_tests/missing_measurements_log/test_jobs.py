import uuid

import pytest

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


class TestMissingMeasurementsLog(BaseJobTests):
    """
    Test class for missing measurements log.
    """

    params = {"orchestration-instance-id": uuid.uuid4()}

    @pytest.fixture(autouse=True, scope="class")
    def job_fixture(
        self,
        environment_configuration: EnvironmentConfiguration,
    ) -> BaseJobFixture:
        return BaseJobFixture(
            environment_configuration=environment_configuration,
            job_name="MissingMeasurementsLog",
            params=self.params,
        )
