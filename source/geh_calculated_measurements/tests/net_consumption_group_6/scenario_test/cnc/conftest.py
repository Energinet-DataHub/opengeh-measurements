from pathlib import Path

import pytest
import yaml
from geh_common.testing.dataframes import (
    read_csv,
)
from geh_common.testing.scenario_testing import TestCase, TestCases
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.net_consumption_group_6.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
)
from geh_calculated_measurements.net_consumption_group_6.domain.cnc_daily_calculation import execute


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest, dummy_logging) -> TestCases:
    """Fixture used for scenario tests. Learn more in package `geh_common`."""

    # Get the path to the scenario
    scenario_path = str(Path(request.module.__file__).parent)

    # Read input data
    current_measurements = read_csv(
        spark,
        f"{scenario_path}/when/measurements_gold/current_v1.csv",
        CurrentMeasurements.schema,
    )

    consumption_metering_point_periods = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__net_consumption_group_6/consumption_metering_point_periods_v1.csv",
        ConsumptionMeteringPointPeriods.schema,
    )

    child_metering_points = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__net_consumption_group_6/child_metering_points_v1.csv",
        ChildMeteringPoints.schema,
    )

    with open(f"{scenario_path}/when/scenario_parameters.yml") as f:
        scenario_parameters = yaml.safe_load(f)

    # Execute the logic
    measurements = execute(
        current_measurements=CurrentMeasurements(current_measurements),
        consumption_metering_point_periods=ConsumptionMeteringPointPeriods(consumption_metering_point_periods),
        child_metering_points=ChildMeteringPoints(child_metering_points),
        time_zone="Europe/Copenhagen",
        execution_start_datetime=scenario_parameters["execution_start_datetime"],
    )

    return TestCases(
        [
            TestCase(expected_csv_path=f"{scenario_path}/then/measurements.csv", actual=measurements.df),
        ]
    )
