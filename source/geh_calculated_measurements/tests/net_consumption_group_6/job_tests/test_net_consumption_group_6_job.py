import os
import sys
import uuid

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.net_consumption_group_6.entry_point import execute
from tests import create_job_environment_variables, create_random_metering_point_id
from tests.net_consumption_group_6.job_tests import get_test_files_folder_path
from tests.net_consumption_group_6.job_tests.seeding import (
    _remove_seeded_electricity_market_data,
    _remove_seeded_gold_data,
    _seed_electricity_market,
    _seed_gold,
)


def test_execute(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
    # gold_table_seeded_randomized_metering_point: None,  # Used implicitly
    # electricity_market_tables_seeded: None,  # Used implicitly
    migrations_executed: None,  # Used implicitly
    external_dataproducts_created: None,  # Used implicitly
    dummy_logging: None,  # Used implicitly
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    monkeypatch.setattr(sys, "argv", ["dummy_script_name", "--orchestration-instance-id", orchestration_instance_id])
    monkeypatch.setattr(os, "environ", create_job_environment_variables(get_test_files_folder_path()))

    parent_metering_point_id = create_random_metering_point_id()
    child_consumption_from_grid_metering_point = create_random_metering_point_id()
    child_net_consumption_metering_point = create_random_metering_point_id()
    child_supply_to_grid_metering_point = create_random_metering_point_id()

    _seed_gold(
        spark,
        parent_metering_point_id,
        child_consumption_from_grid_metering_point,
        child_net_consumption_metering_point,
        child_supply_to_grid_metering_point,
    )

    _seed_electricity_market(
        spark,
        parent_metering_point_id,
        child_consumption_from_grid_metering_point,
        child_net_consumption_metering_point,
        child_supply_to_grid_metering_point,
    )

    # Act
    execute()

    # Assert
    actual_calculated_measurements = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}"
    ).where(F.col(ContractColumnNames.orchestration_instance_id) == orchestration_instance_id)
    assert actual_calculated_measurements.count() > 0

    # Clean up
    _remove_seeded_gold_data(
        spark,
        parent_metering_point_id,
        child_consumption_from_grid_metering_point,
        child_net_consumption_metering_point,
        child_supply_to_grid_metering_point,
    )
    _remove_seeded_electricity_market_data(
        spark,
        parent_metering_point_id,
        child_consumption_from_grid_metering_point,
        child_net_consumption_metering_point,
        child_supply_to_grid_metering_point,
    )
