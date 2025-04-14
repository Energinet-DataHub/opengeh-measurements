import os
import sys
import uuid
from typing import Any

from geh_common.data_products.electricity_market_measurements_input import electrical_heating_child_metering_points_v1
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.infrastructure import (
    CalculatedMeasurementsInternalDatabaseDefinition,
)
from geh_calculated_measurements.electrical_heating.entry_point import execute
from geh_calculated_measurements.net_consumption_group_6.infrastucture.database_definitions import (
    ElectricityMarketMeasurementsInputDatabaseDefinition,
)
from tests import create_job_environment_variables
from tests.electrical_heating.job_tests import get_test_files_folder_path
from tests.electrical_heating.job_tests.seeding import (
    remove_electricity_market_seeding,
    remove_gold_seeding,
    seed_electricity_market,
    seed_gold,
)


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

    seed_gold(spark)
    seed_electricity_market(spark)

    # Act
    execute()

    spark.sql(f"""
        SELECT * FROM {ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{electrical_heating_child_metering_points_v1.view_name} 
    """).show(truncate=False)
    # Assert
    actual = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    assert actual.count() > 0

    # Clean up
    remove_gold_seeding(spark)
    remove_electricity_market_seeding(spark)
