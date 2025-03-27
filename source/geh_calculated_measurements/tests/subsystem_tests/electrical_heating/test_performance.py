import uuid

import pytest

from geh_calculated_measurements.electrical_heating.domain import ChildMeteringPoints, ConsumptionMeteringPointPeriods
from geh_calculated_measurements.electrical_heating.infrastructure.electricity_market.repository import (
    Repository as ElectricityMarketRepository,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements_gold.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from geh_calculated_measurements.testing.utilities.job_tester import JobTester, JobTestFixture
from tests.subsystem_tests.conftest import EnvironmentConfiguration


@pytest.mark.skip(reason="only run if performance test is needed")
class TestElectricalHeating(JobTester):
    """
    Test class for electrical heating.
    """

    @property
    def fixture(self):
        config = EnvironmentConfiguration()
        with pytest.MonkeyPatch.context() as monkeypatch:
            # Patch MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME
            monkeypatch.setattr(
                MeasurementsGoldDatabaseDefinition,
                "DATABASE_NAME",
                config.schema_name,
            )
            monkeypatch.setattr(
                MeasurementsGoldDatabaseDefinition,
                "TIME_SERIES_POINTS_NAME",
                config.time_series_points_table,
            )

            # Patch read_consumption_metering_point_periods
            def patched_read_consumption_metering_point_periods(self) -> ConsumptionMeteringPointPeriods:
                table_name = f"{config.schema_name}.{config.consumption_metering_points_table}"
                df = self._spark.read.format("delta").table(table_name)
                return ConsumptionMeteringPointPeriods(df)

            # Patch read_child_metering_points
            def patched_read_child_metering_points(self) -> ChildMeteringPoints:
                table_name = f"{config.schema_name}.{config.child_metering_points_table}"
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
            return JobTestFixture(
                environment_configuration=config,
                job_name="ElectricalHeating",
                job_parameters={"orchestration-instance-id": uuid.uuid4()},
            )
