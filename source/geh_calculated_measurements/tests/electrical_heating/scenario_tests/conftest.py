from pathlib import Path

import pytest
import yaml
from geh_common.testing.dataframes import read_csv
from geh_common.testing.scenario_testing import TestCase, TestCases
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames, CurrentMeasurements
from geh_calculated_measurements.electrical_heating.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    execute,
)


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest, dummy_logging) -> TestCases:
    """Fixture used for scenario tests. Learn more in package `testcommon.etl`."""

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
        f"{scenario_path}/when/electricity_market__electrical_heating/consumption_metering_point_periods_v1.csv",
        ConsumptionMeteringPointPeriods.schema,
    )
    child_metering_point_periods = read_csv(
        spark,
        f"{scenario_path}/when/electricity_market__electrical_heating/child_metering_points_v1.csv",
        ChildMeteringPoints.schema,
    )

    with open(f"{scenario_path}/when/scenario_parameters.yml") as f:
        scenario_parameters = yaml.safe_load(f)

    # Execute the logic
    actual: DataFrame = execute(
        CurrentMeasurements(current_measurements),
        ConsumptionMeteringPointPeriods(consumption_metering_point_periods),
        ChildMeteringPoints(child_metering_point_periods),
        "Europe/Copenhagen",
        scenario_parameters["execution_start_datetime"],
    )

    # Sort to make the tests deterministic
    actual_df = actual.orderBy(F.col(ContractColumnNames.metering_point_id), F.col(ContractColumnNames.date))

    # Return test cases
    return TestCases(
        [
            # Cache actual in order to prevent the assertion to potentially evaluate the same DataFrame multiple times
            TestCase(expected_csv_path=f"{scenario_path}/then/measurements.csv", actual=actual_df.cache()),
        ]
    )
