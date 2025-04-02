from pyspark.sql import SparkSession

import core.gold.application.streams.gold_measurements_stream as sut
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.gold.infrastructure.config import GoldTableNames
from core.receipts.infrastructure.config.table_names import CoreInternalTableNames
from core.settings.core_internal_settings import CoreInternalSettings
from core.settings.gold_settings import GoldSettings
from core.settings.silver_settings import SilverSettings
from core.silver.infrastructure.config import SilverTableNames
from tests.helpers.builders.silver_measurements_builder import SilverMeasurementsBuilder


def test__stream_measurements_silver_to_gold__append_to_gold_measurements(
    spark: SparkSession, migrations_executed, mock_checkpoint_path
) -> None:
    # Arrange
    metering_point_id = identifier_helper.create_random_metering_point_id()
    silver_measurements = SilverMeasurementsBuilder(spark).add_row(metering_point_id=metering_point_id).build()
    table_helper.append_to_table(
        silver_measurements, SilverSettings().silver_database_name, SilverTableNames.silver_measurements
    )

    # Act
    sut.stream_measurements_silver_to_gold()

    # Arrange
    gold_measurements = spark.table(f"{GoldSettings().gold_database_name}.{GoldTableNames.gold_measurements}").where(
        f"metering_point_id = '{metering_point_id}'"
    )
    assert gold_measurements.count() == 24


def test__stream_measurements_silver_to_gold__append_to_receipts_table(
    spark: SparkSession, migrations_executed, mock_checkpoint_path
) -> None:
    # Arrange
    orchestration_id = "test-receipts-append"
    silver_measurements = SilverMeasurementsBuilder(spark).add_row(orchestration_instance_id=orchestration_id).build()
    table_helper.append_to_table(
        silver_measurements, SilverSettings().silver_database_name, SilverTableNames.silver_measurements
    )

    # Act
    sut.stream_measurements_silver_to_gold()

    # Arrange
    receipts = spark.table(
        f"{CoreInternalSettings.core_internal_database_name}.{CoreInternalTableNames.process_manager_receipts}"
    ).where(f"orchestration_instance_id = '{orchestration_id}'")
    assert receipts.count() == 1
