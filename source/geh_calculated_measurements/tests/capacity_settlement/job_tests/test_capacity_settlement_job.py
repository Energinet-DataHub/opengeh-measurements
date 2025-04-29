import os
import sys
import uuid

from geh_common.pyspark.read_csv import read_csv_path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.capacity_settlement.entry_point import execute
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from tests import create_job_environment_variables
from tests.capacity_settlement.job_tests import TEST_FILES_FOLDER_PATH
from tests.external_data_products import ExternalDataProducts


def _seed_gold_table(spark: SparkSession) -> None:
    database_name = ExternalDataProducts.CURRENT_MEASUREMENTS.database_name
    table_name = ExternalDataProducts.CURRENT_MEASUREMENTS.view_name
    schema = ExternalDataProducts.CURRENT_MEASUREMENTS.schema
    file_name = f"{TEST_FILES_FOLDER_PATH}/{database_name}-{table_name}.csv"
    time_series_points = read_csv_path(spark, file_name, schema)
    time_series_points.write.saveAsTable(
        f"{database_name}.{table_name}",
        format="delta",
        mode="append",
    )


def test_execute(
    spark: SparkSession,
    migrations_executed: None,  # Used implicitly
    external_dataproducts_created: None,  # Used implicitly
    dummy_logging,  # Used implicitly
    monkeypatch,
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "dummy_script_name",
            f"--orchestration-instance-id={orchestration_instance_id}",
            "--calculation-year=2026",
            "--calculation-month=1",
        ],
    )
    monkeypatch.setattr(os, "environ", create_job_environment_variables(str(TEST_FILES_FOLDER_PATH)))
    _seed_gold_table(spark)

    # Act
    execute()

    # Assert
    actual_calculated_measurements = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    actual_calculations = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.CAPACITY_SETTLEMENT_CALCULATIONS_TABLE_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    actual_ten_largest_quantities = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES_TABLE_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    assert actual_calculated_measurements.count() > 0
    assert actual_calculations.count() > 0
    assert actual_ten_largest_quantities.count() > 0
