import uuid
from typing import Any
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.electrical_heating.application import execute_application
from geh_calculated_measurements.electrical_heating.application.environment_variables import (
    EnvironmentVariable,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.calculated_measurements.database_definitions import (
    CalculatedMeasurementsDatabaseDefinition,
)


@pytest.fixture(scope="session")
def job_environment_variables(test_files_folder_path) -> dict:
    return {
        EnvironmentVariable.CATALOG_NAME.name: "spark_catalog",
        EnvironmentVariable.TIME_ZONE.name: "Europe/Copenhagen",
        EnvironmentVariable.ELECTRICITY_MARKET_DATA_PATH.name: test_files_folder_path,
    }


@pytest.mark.skip(reason="Skipping this until write (results) functionality has been implemented")
def test_execute_with_deps(spark: SparkSession, job_environment_variables: dict, seed_gold_table: Any) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    sys_argv = ["dummy_script_name", "--orchestration-instance-id", orchestration_instance_id]

    # Act
    with patch("sys.argv", sys_argv):
        with patch.dict("os.environ", job_environment_variables):
            execute_application()

    # Assert
    actual = spark.read.table(
        f"{CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsDatabaseDefinition.MEASUREMENTS_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    assert actual.count() > 0
