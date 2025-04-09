import uuid

import pytest

from geh_calculated_measurements.testing.utilities.job_tester import JobTest, JobTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.net_consumption_group_6.seed_table import (
    _seed_gold_table,
    delete_seeded_data,
    seed_electricity_market_tables,
)

job_parameters = {"orchestration-instance-id": uuid.uuid4()}


@pytest.mark.skip(reason="The test is failing because the seeded data lacks the date prior the calculation date.")
class TestNetConsumptionGroup6(JobTest):
    @pytest.fixture(scope="class")
    def fixture(
        self,
        parent_metering_point_id: str,
        child_net_consumption_metering_point: str,
        child_supply_to_grid_metering_point: str,
        child_consumption_from_grid_metering_point: str,
    ):
        config = EnvironmentConfiguration()
        # Construct fixture
        base_job_fixture = JobTestFixture(
            environment_configuration=config,
            job_name="NetConsumptionGroup6",
            job_parameters=job_parameters,
        )

        # Remove previously inserted seeded data
        delete_seeded_data(
            base_job_fixture,
            parent_metering_point_id,
            child_net_consumption_metering_point,
            child_supply_to_grid_metering_point,
            child_consumption_from_grid_metering_point,
        )

        # Seed gold table
        _seed_gold_table(base_job_fixture, parent_metering_point_id)

        # Seed electricity market
        seed_electricity_market_tables(
            base_job_fixture,
            parent_metering_point_id,
            child_net_consumption_metering_point,
            child_supply_to_grid_metering_point,
            child_consumption_from_grid_metering_point,
        )

        yield base_job_fixture

        # Remove previously inserted seeded data
        delete_seeded_data(
            base_job_fixture,
            parent_metering_point_id,
            child_net_consumption_metering_point,
            child_supply_to_grid_metering_point,
            child_consumption_from_grid_metering_point,
        )
