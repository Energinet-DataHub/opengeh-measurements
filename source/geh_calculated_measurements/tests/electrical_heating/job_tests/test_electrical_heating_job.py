import uuid
from typing import Any
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.infrastructure import (
    CalculatedMeasurementsInternalDatabaseDefinition,
)
from geh_calculated_measurements.electrical_heating.entry_point import execute


def test_execute(
    spark: SparkSession,
    job_environment_variables: dict,
    seed_gold_table: Any,
    create_calculated_measurements_table: Any,
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    sys_argv = ["dummy_script_name", "--orchestration-instance-id", orchestration_instance_id]

    # Act
    with patch("sys.argv", sys_argv):
        with patch.dict("os.environ", job_environment_variables):
            execute()

    # Assert
    actual = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    assert actual.count() > 0
