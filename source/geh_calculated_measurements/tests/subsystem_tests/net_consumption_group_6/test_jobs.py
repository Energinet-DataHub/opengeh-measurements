import uuid

import pytest

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.net_consumption_group_6.seed_table import (
    _delete_child_seeded_data,
    _delete_parent_seeded_data,
    _seed_child_table,
    _seed_parent_table,
)

job_parameters = {"orchestration-instance-id": uuid.uuid4()}

database = "electricity_market_measurements_input"
parent_table = "net_consumption_group_6_consumption_metering_point_periods_v1"
child_table = "net_consumption_group_6_child_metering_point_periods_v1"

parent_metering_point_id = "170000000000000201"
child_metering_point_id = "150000001500170200"


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
    _delete_parent_seeded_data(
        f"{environment_configuration.catalog_name}.{database}.{parent_table}",
        base_job_fixture,
        parent_metering_point_id,
    )
    _delete_child_seeded_data(
        f"{environment_configuration.catalog_name}.{database}.{child_table}",
        base_job_fixture,
        parent_metering_point_id,
    )

    # Insert seeded data
    _seed_parent_table(
        f"{environment_configuration.catalog_name}.{database}.{parent_table}",
        base_job_fixture,
        parent_metering_point_id,
    )
    _seed_child_table(
        f"{environment_configuration.catalog_name}.{database}.{child_table}",
        base_job_fixture,
        parent_metering_point_id,
        child_metering_point_id,
    )

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
        _delete_parent_seeded_data(
            fully_qualified_table_name=f"{job_fixture.environment_configuration.catalog_name}.{database}.{parent_table}",
            job_fixture=job_fixture,
            parent_metering_point_id=parent_metering_point_id,
        )
        _delete_child_seeded_data(
            fully_qualified_table_name=f"{job_fixture.environment_configuration.catalog_name}.{database}.{child_table}",
            job_fixture=job_fixture,
            parent_metering_point_id=parent_metering_point_id,
        )
