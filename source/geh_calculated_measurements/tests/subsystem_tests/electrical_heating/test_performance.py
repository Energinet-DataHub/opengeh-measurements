import uuid

import pytest

from geh_calculated_measurements.electrical_heating.domain.model.child_metering_points import ChildMeteringPoints
from geh_calculated_measurements.electrical_heating.domain.model.consumption_metering_point_periods import (
    ConsumptionMeteringPointPeriods,
)
from geh_calculated_measurements.electrical_heating.infrastructure.electricity_market.repository import (
    Repository as ElectricityMarketRepository,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements_gold.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from geh_calculated_measurements.testing.utilities.job_tester import JobTest, JobTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


@pytest.mark.skip(reason="only run if performance test is needed")
class TestElectricalHeating(JobTest):
    """
    Test class for electrical heating.
    """

    @pytest.fixture(scope="class")
    def fixture(self):
        config = EnvironmentConfiguration()

        return JobTestFixture(
            environment_configuration=config,
            job_name="ElectricalHeating",
            job_parameters={"orchestration-instance-id": uuid.uuid4()},
        )

    @pytest.fixture(autouse=True, scope="class")
    def patch_repositories(self, fixture: JobTestFixture) -> None:
        environment_configuration = fixture.config
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
