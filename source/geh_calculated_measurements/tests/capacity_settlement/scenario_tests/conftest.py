import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from geh_common.testing.dataframes import (
    read_csv,
)
from geh_common.testing.scenario_testing import TestCase, TestCases
from pyspark.sql import SparkSession

from geh_calculated_measurements.capacity_settlement.application.capacity_settlement_args import CapacitySettlementArgs
from geh_calculated_measurements.capacity_settlement.domain import MeteringPointPeriods, TimeSeriesPoints
from geh_calculated_measurements.capacity_settlement.domain.calculation import execute
from geh_calculated_measurements.capacity_settlement.domain.calculation_output import CalculationOutput
from geh_calculated_measurements.capacity_settlement.infrastructure.electricity_market.schema import (
    metering_point_periods_v1,
)
from geh_calculated_measurements.capacity_settlement.infrastructure.measurements_gold.schema import (
    capacity_settlement_v1,
)
from tests.capacity_settlement.job_tests import create_job_environment_variables


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest) -> TestCases:
    """Fixture used for scenario tests. Learn more in package `testcommon.etl`."""

    # Get the path to the scenario
    scenario_path = str(Path(request.module.__file__).parent)

    # Read input data
    time_series_points = read_csv(
        spark,
        f"{scenario_path}/when/measurements_gold/time_series_points_v1.csv",
        capacity_settlement_v1,
    )
    metering_point_periods = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__capacity_settlement/metering_point_periods_v1.csv",
        metering_point_periods_v1,
    )

    with patch.dict("os.environ", create_job_environment_variables()):
        with open(f"{scenario_path}/when/job_parameters.yml") as f:
            args = yaml.safe_load(f)
        with patch.object(sys, "argv", ["program"] + [f"--{k}={v}" for k, v in args.items()]):
            args = CapacitySettlementArgs()

    # Execute the logic
    calculation_output: CalculationOutput = execute(
        TimeSeriesPoints(time_series_points),
        MeteringPointPeriods(metering_point_periods),
        args.orchestration_instance_id,
        args.calculation_month,
        args.calculation_year,
        args.time_zone,
    )

    # Return test cases
    return TestCases(
        [
            TestCase(
                expected_csv_path=f"{scenario_path}/then/calculations.csv",
                actual=calculation_output.calculations.df,
            ),
            TestCase(
                expected_csv_path=f"{scenario_path}/then/measurements.csv",
                actual=calculation_output.calculated_measurements.df,
            ),
            TestCase(
                expected_csv_path=f"{scenario_path}/then/ten_largest_quantities.csv",
                actual=calculation_output.ten_largest_quantities.df,
            ),
        ]
    )
