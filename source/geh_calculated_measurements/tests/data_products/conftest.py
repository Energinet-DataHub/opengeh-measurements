import os
from pathlib import Path
from typing import Generator

import pytest
from geh_common.testing.dataframes import write_when_files_to_delta
from geh_common.testing.scenario_testing import TestCase, TestCases, get_then_names
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain import CalculatedMeasurements
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsDatabaseDefinition
from geh_calculated_measurements.database_migrations.migrations_runner import migrate
from geh_calculated_measurements.database_migrations.settings.catalog_settings import CatalogSettings
from tests import ensure_databases_created


@pytest.fixture(scope="session")
def migrations_executed(spark: SparkSession, dummy_logging) -> Generator[None, None, None]:
    """Executes all migrations.

    This fixture is useful for all tests that require the migrations to be executed. E.g. when
    a view/dataprodcut/table is required."""

    # Databases are created in dh3infrastructure using terraform
    # So we need to create them in test environment
    ensure_databases_created(spark)

    migrate()
    yield
    for db in dbs:
        spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession, request: pytest.FixtureRequest) -> TestCases:
    # Get the path to the scenario
    scenario_path = str(Path(request.module.__file__).parent)

    # Populate the delta tables with the 'when' files
    path_schema_tuples = [
        (
            "measurements_calculated_internal.calculated_measurements.csv",
            CalculatedMeasurements.schema,
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
    schema = CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME
    with pytest.MonkeyPatch.context() as m:
        m.setattr(os, "environ", {"CATALOG_NAME": "spark_catalog"})
        catalog = CatalogSettings().catalog_name

    schemas = spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
    for schema_row in schemas:
        schema_name = schema_row["namespace"]
        print(f"Schema: {schema_name}")

        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema_name}").collect()
        for table_row in tables:
            print(f"  Table/View: {table_row['tableName']}")

    for path_name in then_files:
        actual = spark.sql(f"SELECT * FROM {catalog}.{schema}.{path_name}")

        test_cases.append(
            TestCase(
                expected_csv_path=f"{scenario_path}/then/{path_name}.csv",
                actual=actual,
            )
        )

    # Return test cases
    return TestCases(test_cases)
