from pathlib import Path

import pytest
from geh_common.testing.dataframes import AssertDataframesConfiguration, assert_dataframes_and_schemas, read_csv
from geh_common.testing.dataframes.write_to_delta import write_when_files_to_delta
from geh_common.testing.scenario_testing import TestCases, get_then_names
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain.model import calculated_measurements
from geh_calculated_measurements.database_migrations.database_definitions import (
    MeasurementsCalculatedInternalDatabaseDefinition,
)


@pytest.mark.parametrize("name", get_then_names())
def test_case(
    spark: SparkSession,
    migrations_executed: None,
    test_cases: TestCases,
    assert_dataframes_configuration: AssertDataframesConfiguration,
    request: pytest.FixtureRequest,
    name: str,
) -> None:
    # Arrange / act?
    test_case = test_cases[name]

    if 1 == 0:
        scenario_path = str(Path(request.module.__file__).parent.parent)
        df = read_csv(
            spark, f"{scenario_path}/wip_xhtca/when/input.csv", calculated_measurements.calculated_measurements_schema
        )

        catalog = "spark_catalog"
        schema = MeasurementsCalculatedInternalDatabaseDefinition.measurements_calculated_internal_database
        table = MeasurementsCalculatedInternalDatabaseDefinition.MEASUREMENTS_NAME
        df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table}")
        # measurements_calculated_internal.measurements.
        actual = spark.table("test_view")

        # Populate the delta tables with the 'when' files.
        path_schema_tuples = [
            (
                "wholesale_basis_data_internal.charge_link_periods.csv",
                calculated_measurements.calculated_measurements_schema,
            )
        ]
        write_when_files_to_delta(
            spark,
            scenario_path,
            path_schema_tuples,
        )
        actual = spark.table("test_view")

        # expected = read_csv(
        #     spark,
        #     name.
        #     f"{scenario_path}/wip_xhtca/then/output.csv",
        #     calculated_measurements.calculated_measurements_schema,
        # )

    # Assert
    assert_dataframes_and_schemas(
        actual=test_case.actual,
        expected=test_case.expected,
        configuration=assert_dataframes_configuration,
    )
