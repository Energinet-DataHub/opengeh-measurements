import uuid
from typing import Any
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.capacity_settlement.entry_point import execute
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition


def _get_job_parameters(orchestration_instance_id: str) -> list[str]:
    return [
        "dummy_script_name",
        f"--orchestration-instance-id={orchestration_instance_id}",
        "--calculation-year=2026",
        "--calculation-month=1",
    ]


def test_execute(spark: SparkSession, job_environment_variables: dict, seed_gold_table: Any) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())

    # Act
    with patch("sys.argv", _get_job_parameters(orchestration_instance_id)):
        with patch.dict("os.environ", job_environment_variables):
            execute()

    # Assert
    actual = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    assert actual.count() > 0
