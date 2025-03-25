import uuid

import pytest

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.net_consumption_group_6.seed_table import _delete_seeded_data, _seed_table

job_parameters = {"orchestration-instance-id": uuid.uuid4()}


@pytest.fixture(scope="session")
def job_fixture(
    environment_configuration: EnvironmentConfiguration,
) -> BaseJobFixture:
    # Construct fixture
    base_job_fixture = BaseJobFixture(
        environment_configuration=environment_configuration,
        job_name="NetConsumptionGroup6",
        job_parameters=job_parameters,
    )

    # Remove previously inserted seeded data
    _delete_seeded_data(job_fixture=base_job_fixture)

    # Insert seeded data
    _seed_table(job_fixture=base_job_fixture)

    return base_job_fixture


class TestNetConsumptionGroup6(BaseJobTests):
    """
    Test class for net consumption for group 6.
    """

    @pytest.mark.skip(reason="Skipped due to issues with the telemetry data not available in the logs.")
    def test__and_then_job_telemetry_is_created(self, job_fixture: BaseJobFixture) -> None:
        pass

    @pytest.mark.order("last")
    def test__remove_seeded_data(self, job_fixture: BaseJobFixture) -> None:
        _delete_seeded_data(job_fixture=job_fixture)
