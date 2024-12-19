from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from testcommon.etl import read_csv

from electrical_heating.domain.calculation import _execute
from integration_tests.calculation_scenarios.testcommon import TestCases, TestCase


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request) -> TestCases:
    scenario_path = str(Path(request.module.__file__).parent)

    # Read input data
    time_series_points = read_csv(spark, f"{scenario_path}/when/time_series_points.csv")
    consumption_metering_point_periods = read_csv(spark, f"{scenario_path}/when/electrical_heating_internal/consumption_metering_point_periods.csv")
    child_metering_point_periods = read_csv(spark, f"{scenario_path}/when/electrical_heating_internal/child_metering_point_periods.csv")

    # Execute the calculation logic
    actual_measurements = _execute(time_series_points, consumption_metering_point_periods, child_metering_point_periods, "Europe/Copenhagen")

    # Read expected data
    expected_measurements = read_csv(spark, f"{scenario_path}/then/measurements.csv")

    # Return test cases
    return TestCases([TestCase("measurements", actual_measurements, expected_measurements)])
