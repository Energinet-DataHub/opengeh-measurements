import os
import sys
import uuid
from typing import Any

from geh_common.pyspark.read_csv import read_csv_path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.infrastructure import (
    CalculatedMeasurementsInternalDatabaseDefinition,
)
from geh_calculated_measurements.electrical_heating.entry_point import execute
from tests import create_job_environment_variables
from tests.electrical_heating.job_tests import get_test_files_folder_path
<<<<<<< HEAD
from tests.external_data_products import ExternalDataProducts


def _seed_gold_table(
    spark: SparkSession,
) -> None:
    database_name = ExternalDataProducts.CURRENT_MEASUREMENTS.database_name
    table_name = ExternalDataProducts.CURRENT_MEASUREMENTS.view_name
    schema = ExternalDataProducts.CURRENT_MEASUREMENTS.schema
    file_name = f"{get_test_files_folder_path()}/{database_name}-{table_name}.csv"
    time_series_points = read_csv_path(spark, file_name, schema)
    time_series_points.write.saveAsTable(
        f"{database_name}.{table_name}",
        format="delta",
        mode="append",
    )
=======
from tests.electrical_heating.job_tests.seeding import seed_electricity_market, seed_gold
>>>>>>> 7addf24b0f0bbbc82030b5cbc179aa71b6cf7e3f


def test_execute(
    spark: SparkSession,
    migrations_executed: None,  # Used implicitly
    external_dataproducts_created: None,  # Used implicitly
    dummy_logging: Any,  # Used implicitly
    monkeypatch,
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    monkeypatch.setattr(sys, "argv", ["dummy_script_name", "--orchestration-instance-id", orchestration_instance_id])
    monkeypatch.setattr(os, "environ", create_job_environment_variables(get_test_files_folder_path()))
    _seed_gold_table(spark)

    seed_gold(spark)
    seed_electricity_market(spark)

    # Act
    execute()

    # Assert
    actual = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    assert actual.count() > 0
