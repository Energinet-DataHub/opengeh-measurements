from pathlib import Path

import pytest
import yaml
from geh_common.testing.dataframes import (
    read_csv,
)
from geh_common.testing.scenario_testing import TestCase, TestCases
from pyspark.sql import SparkSession

from geh_calculated_measurements.capacity_settlement.domain import MeteringPointPeriods, TimeSeriesPoints
from geh_calculated_measurements.capacity_settlement.domain.calculation import execute
from geh_calculated_measurements.capacity_settlement.domain.calculation_output import CalculationOutput
from geh_calculated_measurements.capacity_settlement.infrastructure.electricity_market.schema import (
    metering_point_periods_v1,
)
from geh_calculated_measurements.common.domain import CurrentMeasurements


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest, dummy_logging) -> TestCases:
    """Fixture used for scenario tests. Learn more in package `testcommon.etl`."""

    # Get the path to the scenario
    scenario_path = str(Path(request.module.__file__).parent)

    # Read input data
    time_series_points = read_csv(
        spark,
        f"{scenario_path}/when/measurements_gold/current_v1.csv",
        CurrentMeasurements.schema,
    )
    metering_point_periods = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__capacity_settlement/metering_point_periods_v1.csv",
        metering_point_periods_v1,
    )

    with open(f"{scenario_path}/when/scenario_parameters.yml") as f:
        scenario_parameters = yaml.safe_load(f)

    # Execute the logic
    calculation_output: CalculationOutput = execute(
        TimeSeriesPoints(time_series_points),
        MeteringPointPeriods(metering_point_periods),
        scenario_parameters["calculation_month"],
        scenario_parameters["calculation_year"],
        "Europe/Copenhagen",
    )

    # Return test cases
    return TestCases(
        [
            TestCase(
                expected_csv_path=f"{scenario_path}/then/measurements.csv",
                actual=calculation_output.calculated_measurements_daily,
            ),
            TestCase(
                expected_csv_path=f"{scenario_path}/then/ten_largest_quantities.csv",
                actual=calculation_output.ten_largest_quantities.df,
            ),
        ]
    )
