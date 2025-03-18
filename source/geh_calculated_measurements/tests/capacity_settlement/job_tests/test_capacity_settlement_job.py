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
    monkeypatch: pytest.MonkeyPatch,
    gold_table_seeded: Any,  # Used implicitly
    calculated_measurements_table_created: Any,  # Used implicitly
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
