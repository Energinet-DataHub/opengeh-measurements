from pathlib import Path

import pytest
from geh_common.testing.dataframes import write_when_files_to_delta
from geh_common.testing.scenario_testing import TestCase, TestCases, get_then_names
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.application.model import CalculatedMeasurementsInternal
from geh_calculated_measurements.database_migrations import DatabaseNames
from tests import SPARK_CATALOG_NAME


@pytest.fixture(scope="module")
def test_cases(
    spark: SparkSession,
    request: pytest.FixtureRequest,
    migrations_executed: None,  # Used implicitly
) -> TestCases:
    # Get the path to the scenario
    scenario_path = str(Path(request.module.__file__).parent)

    # Populate the delta tables with the 'when' files
    path_schema_tuples = [
        (
            "measurements_calculated_internal.calculated_measurements.csv",
            CalculatedMeasurementsInternal.schema,
        )
    ]
    write_when_files_to_delta(
        spark,
        scenario_path,
        path_schema_tuples,
    )

    # Receive a list of 'then' file names
    then_files = get_then_names(scenario_path)

    # Construct a list of TestCase objects
    test_cases = []

    for path_name in then_files:
        actual = spark.sql(f"SELECT * FROM {SPARK_CATALOG_NAME}.{DatabaseNames.MEASUREMENTS_CALCULATED}.{path_name}")

        test_cases.append(
            TestCase(
                expected_csv_path=f"{scenario_path}/then/{path_name}.csv",
                actual=actual,
            )
        )

    # Return test cases
    return TestCases(test_cases)
