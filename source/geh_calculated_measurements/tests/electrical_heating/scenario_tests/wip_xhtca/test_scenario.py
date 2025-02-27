from pathlib import Path

import pytest
from geh_common.testing.dataframes import AssertDataframesConfiguration, assert_dataframes_and_schemas, read_csv
from pyspark.sql import SparkSession

from geh_calculated_measurements.database_migrations.database_definitions import (
    MeasurementsCalculatedInternalDatabaseDefinition,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.calculated_measurements.schema import (
    calculated_measurements_schema,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.measurements_gold.schema import (
    time_series_points_v1,
)


def test_case(
    spark: SparkSession,
    migrations_executed,
    assert_dataframes_configuration: AssertDataframesConfiguration,
    request: pytest.FixtureRequest,
) -> None:
    # Arrange / act?
    scenario_path = str(Path(request.module.__file__).parent.parent)
    # df = spark.read.csv(
    #     f"{scenario_path}/wip_xhtca/when/input.csv",
    #     header=True,
    #     schema=calculated_measurements_schema,
    #     sep=";",
    # )
    df = read_csv(
        spark,
        f"{scenario_path}/wip_xhtca/when/input.csv",
        calculated_measurements_schema,
    )

    # print("df:", df.show())
    # if 1 == 0:
    # sparkconf = substitutions()
    # substitution_variables = substitutions.substitutions()
    # print(substitution_variables)
    catalog = "spark_catalog"
    schema = MeasurementsCalculatedInternalDatabaseDefinition.measurements_calculated_internal_database
    table = MeasurementsCalculatedInternalDatabaseDefinition.MEASUREMENTS_NAME
    # catalog = sparkconf.
    # schema = sparkconf.migration_schema_name
    # table = sparkconf.migration_table_name
    # print(f"path: {catalog}.{schema}.{table}")
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table}")

    # test = spark.sql(f"SELECT * FROM {catalog}.{schema}.{table}")
    # print(test.show())

    actual = spark.sql("SELECT * FROM test_view")
    # print(actual.show())

    expected = read_csv(
        spark,
        f"{scenario_path}/wip_xhtca/then/output.csv",
        time_series_points_v1,
    )

    # Assert
    assert_dataframes_and_schemas(
        actual=actual,
        expected=expected,
        configuration=assert_dataframes_configuration,
    )
