import uuid

import pytest

from geh_calculated_measurements.electrical_heating.domain import ChildMeteringPoints, ConsumptionMeteringPointPeriods
from geh_calculated_measurements.electrical_heating.infrastructure.electricity_market.repository import (
    Repository as ElectricityMarketRepository,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements_gold.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.conftest import EnvironmentConfiguration


@pytest.mark.skip(reason="only run if performance test is needed")
class TestElectricalHeatingPerformance(BaseJobTests):
    """
    Test with performance configuration using delta tables
    """

    params = {"orchestration-instance-id": uuid.uuid4()}
    fixture = None

    def get_or_create_fixture(self, environment_configuration: EnvironmentConfiguration) -> BaseJobFixture:
        if self.fixture is None:
            self.fixture = BaseJobFixture(
                environment_configuration=environment_configuration,
                job_name="ElectricalHeating",
                params=self.params,
            )
        return self.fixture

    @pytest.fixture(autouse=True, scope="class")
    def setup_fixture(self, environment_configuration: EnvironmentConfiguration) -> None:
        """Set up the fixture for the test class."""
        return self.get_or_create_fixture(environment_configuration)

    @pytest.fixture(autouse=True, scope="class")
    def patch_repositories(self, environment_configuration: EnvironmentConfiguration) -> None:
        monkeypatch = pytest.MonkeyPatch()
        # Patch MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME
        monkeypatch.setattr(
            MeasurementsGoldDatabaseDefinition,
            "DATABASE_NAME",
            environment_configuration.schema_name,
        )
        monkeypatch.setattr(
            MeasurementsGoldDatabaseDefinition,
            "TIME_SERIES_POINTS_NAME",
            environment_configuration.time_series_points_table,
        )

        # Patch read_consumption_metering_point_periods
        def patched_read_consumption_metering_point_periods(self) -> ConsumptionMeteringPointPeriods:
            table_name = (
                f"{environment_configuration.schema_name}.{environment_configuration.consumption_metering_points_table}"
            )
            df = self._spark.read.format("delta").table(table_name)
            return ConsumptionMeteringPointPeriods(df)

        # Patch read_child_metering_points
        def patched_read_child_metering_points(self) -> ChildMeteringPoints:
            table_name = (
                f"{environment_configuration.schema_name}.{environment_configuration.child_metering_points_table}"
            )
            df = self._spark.read.format("delta").table(table_name)
            return ChildMeteringPoints(df)

        monkeypatch.setattr(
            ElectricityMarketRepository,
            "read_consumption_metering_point_periods",
            patched_read_consumption_metering_point_periods,
        )
        monkeypatch.setattr(
            ElectricityMarketRepository, "read_child_metering_points", patched_read_child_metering_points
        )
