import uuid

import pytest

from geh_calculated_measurements.testing.utilities.job_tester import JobTest, JobTestFixture
from tests import create_random_metering_point_id
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.net_consumption_group_6.seed_table import (
    _seed_gold_table,
    delete_seeded_data,
    seed_electricity_market_tables,
)

job_parameters = {"orchestration-instance-id": uuid.uuid4()}


class TestNetConsumptionGroup6(JobTest):
    @pytest.fixture(scope="class")
    def fixture(self):
        config = EnvironmentConfiguration()

        # Construct fixture
        base_job_fixture = JobTestFixture(
            environment_configuration=config,
            job_name="NetConsumptionGroup6",
            job_parameters=job_parameters,
        )

        # Generate random metering point ids
        parent_metering_point_id = create_random_metering_point_id()
        child_net_consumption_metering_point = create_random_metering_point_id()
        child_supply_to_grid_metering_point = create_random_metering_point_id()
        child_consumption_from_grid_metering_point = create_random_metering_point_id()

        # Seed gold table
        _seed_gold_table(
            base_job_fixture,
            parent_metering_point_id,
            child_supply_to_grid_metering_point,
            child_consumption_from_grid_metering_point,
        )

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
