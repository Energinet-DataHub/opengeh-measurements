import uuid
from typing import Any

import pytest

from geh_calculated_measurements.common.infrastructure import CurrentMeasurementsRepository
from geh_calculated_measurements.electrical_heating.domain.model.child_metering_points import ChildMeteringPoints
from geh_calculated_measurements.electrical_heating.domain.model.consumption_metering_point_periods import (
    ConsumptionMeteringPointPeriods,
)
from geh_calculated_measurements.electrical_heating.infrastructure import ElectricityMarketRepository
from geh_calculated_measurements.testing import JobTest, JobTestFixture
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
    def patch_repositories(self, fixture: JobTestFixture) -> Any:
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(
                CurrentMeasurementsRepository,
                "database_name",
                fixture.config.schema_name,
            )
            monkeypatch.setattr(
                CurrentMeasurementsRepository,
                "table_name",
                fixture.config.time_series_points_table,
            )

            # Patch read_consumption_metering_point_periods
            def patched_read_consumption_metering_point_periods(self) -> ConsumptionMeteringPointPeriods:
                table_name = f"{fixture.config.schema_name}.{fixture.config.consumption_metering_points_table}"
                df = self._spark.read.format("delta").table(table_name)
                return ConsumptionMeteringPointPeriods(df)

            # Patch read_child_metering_points
            def patched_read_child_metering_points(self) -> ChildMeteringPoints:
                table_name = f"{fixture.config.schema_name}.{fixture.config.child_metering_points_table}"
                df = self._spark.read.format("delta").table(table_name)
                return ChildMeteringPoints(df)

            monkeypatch.setattr(
                ElectricityMarketRepository,
                "read_consumption_metering_point_periods",
                patched_read_consumption_metering_point_periods,
            )
            monkeypatch.setattr(
                ElectricityMarketRepository,
                "read_child_metering_points",
                patched_read_child_metering_points,
            )
            yield
