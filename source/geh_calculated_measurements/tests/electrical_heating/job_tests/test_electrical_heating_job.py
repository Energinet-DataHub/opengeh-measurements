import os
import sys
import uuid
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.infrastructure import (
    CalculatedMeasurementsInternalDatabaseDefinition,
)
from tests.electrical_heating.job_tests import get_test_files_folder_path


def test_execute(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
    gold_table_seeded: Any,  # Used implicitly
    calculated_measurements_table_created: Any,  # Used implicitly
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    sys_argv = ["dummy_script_name", "--orchestration-instance-id", orchestration_instance_id]

    # Act
    monkeypatch.setattr(sys, "argv", sys_argv)
    monkeypatch.setattr(
        os,
        "environ",
        {
            "CATALOG_NAME": "spark_catalog",
            "TIME_ZONE": "Europe/Copenhagen",
            "ELECTRICITY_MARKET_DATA_PATH": get_test_files_folder_path(),
            "DATABASE_MEASUREMENTS_CALCULATED_INTERNAL": "measurements_calculated_internal",
        },
    )

    # Assert
    actual = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition().DATABASE_MEASUREMENTS_CALCULATED_INTERNAL}.{CalculatedMeasurementsInternalDatabaseDefinition().MEASUREMENTS_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    assert actual.count() > 0
