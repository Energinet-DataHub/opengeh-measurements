### This file contains the fixtures that are used in the tests. ###
from datetime import datetime
from pathlib import Path
from uuid import UUID

import pytest
from geh_common.domain.types import MeteringPointType, OrchestrationType
from geh_common.testing.dataframes import read_csv
from geh_common.testing.scenario_testing import TestCase, TestCases
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.application.model import calculated_measurements_factory
from geh_calculated_measurements.common.application.model.calculated_measurements_factory import (
    calculated_measurements_daily_schema,
)


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest) -> TestCases:
    # Get the path to the scenario
    scenario_path = str(Path(request.module.__file__).parent)

    # Read input data
    calculated_measurements_daily = read_csv(
        spark,
        f"{scenario_path}/when/calculated_measurements_daily.csv",
        calculated_measurements_daily_schema,
    )

    # Execute the logic to be tested
    actual = calculated_measurements_factory.create(
        calculated_measurements_daily,
        UUID("00000000-0000-0000-0000-000000000001"),
        OrchestrationType.ELECTRICAL_HEATING,
        MeteringPointType.ELECTRICAL_HEATING,
        "Europe/Copenhagen",
        datetime.fromisoformat("2025-03-31T12:34:56+00:00"),
    )

    # Return test cases
    return TestCases(
        [
            TestCase(
                expected_csv_path=f"{scenario_path}/then/calculated_measurements_hourly.csv",
                actual=actual.df,
            )
        ]
    )
