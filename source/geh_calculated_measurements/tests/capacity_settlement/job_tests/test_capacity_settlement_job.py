import uuid
from typing import Any
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.capacity_settlement.entry_point import execute
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from tests.capacity_settlement.job_tests import create_job_environment_variables


def _get_job_parameters(orchestration_instance_id: str) -> list[str]:
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

    # Act
    with pytest.MonkeyPatch.context() as ctx:
        ctx.setenv("DATABASE_MEASUREMENTS_CALCULATED_INTERNAL", "measurements_calculated_internal")
        with patch("sys.argv", _get_job_parameters(orchestration_instance_id)):
            with patch.dict("os.environ", create_job_environment_variables()):
                execute()
        # Assert
        with pytest.MonkeyPatch.context() as ctx:
            ctx.setenv("DATABASE_MEASUREMENTS_CALCULATED_INTERNAL", "measurements_calculated_internal")
            actual_calculated_measurements = spark.read.table(
                f"{CalculatedMeasurementsInternalDatabaseDefinition().DATABASE_MEASUREMENTS_CALCULATED_INTERNAL}.{CalculatedMeasurementsInternalDatabaseDefinition().MEASUREMENTS_NAME}"
            ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
            actual_calculations = spark.read.table(
                f"{CalculatedMeasurementsInternalDatabaseDefinition().DATABASE_MEASUREMENTS_CALCULATED_INTERNAL}.{CalculatedMeasurementsInternalDatabaseDefinition().CAPACITY_SETTLEMENT_CALCULATIONS_NAME}"
            ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
            actual_ten_largest_quantities = spark.read.table(
                f"{CalculatedMeasurementsInternalDatabaseDefinition().DATABASE_MEASUREMENTS_CALCULATED_INTERNAL}.{CalculatedMeasurementsInternalDatabaseDefinition().CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES_NAME}"
            ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
            assert actual_calculated_measurements.count() > 0
            assert actual_calculations.count() > 0
            assert actual_ten_largest_quantities.count() > 0
