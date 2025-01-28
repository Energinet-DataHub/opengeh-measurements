import uuid
from typing import Any
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import opengeh_electrical_heating.application.execute_with_deps as execute_with_deps
from opengeh_electrical_heating.application.job_args.environment_variables import EnvironmentVariable
from opengeh_electrical_heating.infrastructure.measurements_bronze.database_definitions import (
    MeasurementsBronzeDatabase,
)


@pytest.fixture(scope="session")
def job_environment_variables() -> dict:
    return {
        EnvironmentVariable.CATALOG_NAME.name: "spark_catalog",
        EnvironmentVariable.TIME_ZONE.name: "Europe/Copenhagen",
    }


def test_execute_with_deps(
    spark: SparkSession, job_environment_variables: dict, write_test_data_to_gold_table: Any
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())

    sys_argv = ["dummy_script_name", "--orchestration-instance-id", orchestration_instance_id]

    # Act
    with patch("sys.argv", sys_argv):
        with patch.dict("os.environ", job_environment_variables):
            execute_with_deps.execute_with_deps()

    # Assert
    actual = spark.read.table(
        f"{MeasurementsBronzeDatabase.DATABASE_NAME}.{MeasurementsBronzeDatabase.MEASUREMENTS_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    assert actual.count() > 0
