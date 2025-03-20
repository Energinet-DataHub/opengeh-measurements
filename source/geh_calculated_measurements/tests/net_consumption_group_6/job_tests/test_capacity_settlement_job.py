import uuid
from typing import Any
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.net_consumption_group_6.entry_point import execute
from tests.net_consumption_group_6.job_tests import create_job_environment_variables


def _create_job_parameters(orchestration_instance_id: str) -> list[str]:
    return [
        "dummy_script_name",
        f"--orchestration-instance-id={orchestration_instance_id}",
        "--calculation-year=2026",
        "--calculation-month=1",
    ]


def test_execute(
    spark: SparkSession,
    gold_table_seeded: Any,  # Used implicitly
    calculated_measurements_table_created: Any,  # Used implicitly
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())

    with patch("sys.argv", _create_job_parameters(orchestration_instance_id)):
        with patch.dict("os.environ", create_job_environment_variables()):
            # Act
            execute()

    return

    # The following assertions will be added when the job begins to store the results of the calculation

    # Assert: Some data is written to the calculated measurements table
    actual_calculated_measurements = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    assert actual_calculated_measurements.count() > 0

    # Assert: Some data is written to the cenc table
    actual_cenc = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.CALCULATED_ESTIMATED_ANNUAL_CONSUMPTION_TABLE_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    assert actual_cenc.count() > 0
