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
from geh_calculated_measurements.net_consumption_group_6.domain.cenc_calculation import execute


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
    cenc, measurements = execute(
        CurrentMeasurements(current_measurements),
        ConsumptionMeteringPointPeriods(consumption_metering_point_periods),
        ChildMeteringPoints(child_metering_points),
        "Europe/Copenhagen",
        scenario_parameters["execution_start_datetime"],
    )

    # Return test cases
    test_cases_list = []
    cenc_csv_path = Path(f"{scenario_path}/then/cenc.csv")
    if cenc_csv_path.exists():
        test_cases_list.append(
            TestCase(
                expected_csv_path=str(cenc_csv_path),
                actual=cenc.df,
            )
        )

    measurements_csv_path = Path(f"{scenario_path}/then/measurements.csv")
    if measurements_csv_path.exists():
        test_cases_list.append(
            TestCase(
                expected_csv_path=str(measurements_csv_path),
                actual=measurements.df,
            ),
        )
    return TestCases(test_cases_list)
