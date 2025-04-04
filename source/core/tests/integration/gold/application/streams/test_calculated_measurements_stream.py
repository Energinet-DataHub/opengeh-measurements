from pyspark.sql import SparkSession

import core.gold.application.streams.calculated_measurements_stream as sut
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.gold.infrastructure.config import GoldTableNames
from core.gold.infrastructure.config.external_view_names import ExternalViewNames
from core.settings.calculated_settings import CalculatedSettings
from tests.helpers.builders.calculated_builder import CalculatedMeasurementsBuilder


def test__stream_measurements_calculated_to_gold__append_to_gold_measurements(
    spark: SparkSession, create_external_resources, migrations_executed, mock_checkpoint_path
) -> None:
    """
    Note: We're appending to a table instead of a view, as streaming from views isn't supported in native Spark—
    only in Databricks Runtime. A subsystem test will cover the view case.
    """
    # Arrange
    calculated_database_name = CalculatedSettings.calculated_database_name
    metering_point_id = identifier_helper.create_random_metering_point_id()
    calculated_measurements = CalculatedMeasurementsBuilder(spark).add_row(metering_point_id=metering_point_id).build()
    table_helper.append_to_table(
        calculated_measurements, calculated_database_name, ExternalViewNames.calculated_measurements_v1
    )

    # Act
    sut.stream_measurements_calculated_to_gold()

    # Arrange
    gold_measurements = spark.table(f"{calculated_database_name}.{GoldTableNames.gold_measurements}").where(
        f"metering_point_id = '{metering_point_id}'"
    )
    assert gold_measurements.count() == 1
