import os
import sys
import uuid
from typing import Any

from geh_common.pyspark.read_csv import read_csv_path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from geh_calculated_measurements.common.domain import ContractColumnNames, CurrentMeasurements
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from geh_calculated_measurements.electrical_heating.entry_point import execute
from tests import create_job_environment_variables, create_random_metering_point_id
from tests.electrical_heating.job_tests import get_test_files_folder_path

generate_metering_point_id_udf = F.udf(create_random_metering_point_id, StringType())


def _seed_gold(spark: SparkSession) -> None:
    file_name = f"{get_test_files_folder_path()}/{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}-{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}.csv"
    # Read csv
    time_series_points = read_csv_path(spark, file_name, CurrentMeasurements.schema)

    # Get unique metering_point_ids
    distinct_ids = time_series_points.select("metering_point_id").distinct()

    # Add a new randomized ID per original
    randomized_ids = distinct_ids.withColumn("new_metering_point_id", generate_metering_point_id_udf())

    # Join back to original DataFrame
    time_series_points = time_series_points.join(randomized_ids, on="metering_point_id", how="left")
    time_series_points = time_series_points.select(
        F.col("new_metering_point_id").alias("metering_point_id"),
        "observation_time",
        "quantity",
        "quality",
        "metering_point_type",
    )

    time_series_points.write.saveAsTable(
        f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}",
        format="delta",
        mode="append",
    )


def test_execute(
    spark: SparkSession,
    gold_table_seeded,
    gold_table_seeded_2,
    dummy_logging: Any,  # Used implicitly
    migrations_executed: None,  # Used implicitly
    external_dataproducts_created: None,  # Used implicitly
    monkeypatch,
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    monkeypatch.setattr(sys, "argv", ["dummy_script_name", "--orchestration-instance-id", orchestration_instance_id])
    monkeypatch.setattr(os, "environ", create_job_environment_variables(get_test_files_folder_path()))

    # _seed_gold(spark)
    print("A:")
    spark.sql(
        f"""SELECT * FROM {MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}"""
    ).show(truncate=False)

    # Act
    execute()
    print("B:")
    spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}"
    ).show(truncate=False)

    # Assert
    actual = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}"
    ).where(F.col(ContractColumnNames.orchestration_instance_id) == orchestration_instance_id)
    assert actual.count() > 0

    # Clean up
    spark.sql(f"""
        DELETE FROM {CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}
        WHERE orchestration_instance_id = '{orchestration_instance_id}'
    """)

    # spark.sql(f"""
    #             SELECT * FROM {MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}
    #           """).show(truncate=False)
