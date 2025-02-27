from pathlib import Path

import pytest
from geh_common.testing.dataframes import AssertDataframesConfiguration, assert_dataframes_and_schemas, read_csv
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain.model import calculated_measurements
from geh_calculated_measurements.database_migrations.database_definitions import (
    MeasurementsCalculatedInternalDatabaseDefinition,
)

# from geh_calculated_measurements.electrical_heating.infrastructure.measurements.measurements_gold.schema import (
#     time_series_points_v1,
# )


def test_case(
    spark: SparkSession,
    migrations_executed,
    assert_dataframes_configuration: AssertDataframesConfiguration,
    request: pytest.FixtureRequest,
) -> None:
    # Arrange / act?
    scenario_path = str(Path(request.module.__file__).parent.parent)

    df = read_csv(
        spark, f"{scenario_path}/wip_xhtca/when/input.csv", calculated_measurements.calculated_measurements_schema
    )

    catalog = "spark_catalog"
    schema = MeasurementsCalculatedInternalDatabaseDefinition.measurements_calculated_internal_database
    table = MeasurementsCalculatedInternalDatabaseDefinition.MEASUREMENTS_NAME
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table}")

    actual = spark.sql(
        """SELECT 
            orchestration_type,
            orchestration_instance_id,
            metering_point_id,
            transaction_id,
            transaction_creation_datetime,
            metering_point_type,
            date,
            quantity
        FROM test_view"""
    )

    expected = read_csv(
        spark,
        f"{scenario_path}/wip_xhtca/then/output.csv",
        calculated_measurements.calculated_measurements_schema,
    )

    # Assert
    assert_dataframes_and_schemas(
        actual=actual,
        expected=expected,
        configuration=assert_dataframes_configuration,
    )
