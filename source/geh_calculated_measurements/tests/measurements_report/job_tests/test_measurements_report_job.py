import sys
import uuid

import pytest
from pyspark.sql import SparkSession

from geh_calculated_measurements.measurements_report.entry_point import execute


def test_execute(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
    migrations_executed: None,  # Used implicitly
    external_dataproducts_created: None,  # Used implicitly
    dummy_logging: None,  # Used implicitly
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    monkeypatch.setattr(sys, "argv", ["dummy_script_name", "--orchestration-instance-id", orchestration_instance_id])

    # Act
    execute()

    # Assert
    # TODO HENRIK when we can write to to table we add test
    # actual_calculated_measurements = spark.read.table(
    #     f"{InternalTables.CALCULATED_MEASUREMENTS.database_name}.{InternalTables.CALCULATED_MEASUREMENTS.table_name}"
    # ).where(F.col(ContractColumnNames.orchestration_instance_id) == orchestration_instance_id)
    # assert actual_calculated_measurements.count() > 0
