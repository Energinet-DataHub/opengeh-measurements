from pathlib import Path

import pytest
import yaml
from geh_common.testing.dataframes import read_csv
from geh_common.testing.scenario_testing import TestCase, TestCases
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames, CurrentMeasurements
from geh_calculated_measurements.missing_measurements_log.domain import MeteringPointPeriods, execute
from tests.external_data_products import ExternalDataProducts


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest, dummy_logging) -> TestCases:
    """Fixture used for scenario tests. Learn more in package `testcommon.etl`."""

    # Get the path to the scenario
    scenario_path = str(Path(request.module.__file__).parent)

    # Read input data
    current_measurements = read_csv(
        spark,
        f"{scenario_path}/when/measurements_gold/current_v1.csv",
        ExternalDataProducts.CURRENT_MEASUREMENTS.schema,
    )
    metering_point_periods = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__missing_measurements_log/metering_point_periods_v1.csv",
        ExternalDataProducts.MISSING_MEASUREMENTS_LOG_METERING_POINT_PERIODS.schema,
    )

    with open(f"{scenario_path}/when/scenario_parameters.yml") as f:
        scenario_parameters = yaml.safe_load(f)

    # Execute the logic
    actual = execute(
        current_measurements=CurrentMeasurements(current_measurements),
        metering_point_periods=MeteringPointPeriods(metering_point_periods),
        grid_area_codes=scenario_parameters["grid_area_codes"],
        orchestration_instance_id=scenario_parameters["orchestration_instance_id"],
        time_zone="Europe/Copenhagen",
    )

    # Sort to make the tests deterministic
    actual_df = actual.orderBy(F.col(ContractColumnNames.metering_point_id), F.col(ContractColumnNames.date))

    # Return test cases
    return TestCases(
        [
            # Cache actual in order to prevent the assertion to potentially evaluate the same DataFrame multiple times
            TestCase(expected_csv_path=f"{scenario_path}/then/missing_measurements_log.csv", actual=actual_df.cache()),
        ]
    )
