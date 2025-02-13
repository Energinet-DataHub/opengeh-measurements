import random
from unittest import mock

from pyspark.sql import SparkSession

from core.gold.infrastructure.adapters.delta_gold_adapter import DeltaGoldAdapter
from core.gold.infrastructure.config import GoldTableNames
from core.settings.catalog_settings import CatalogSettings
from tests.gold.helpers.gold_builder import GoldMeasurementsDataFrameBuilder


@mock.patch("os.getenv")
@mock.patch("core.gold.infrastructure.adapters.delta_gold_adapter.get_checkpoint_path")
def test__start_write_stream__should_write_to_gold_table(
    mock_get_checkpoint_path, mock_getenv, spark: SparkSession, migrations_executed
):
    # Arrange
    gold_adapter = DeltaGoldAdapter()
    gold_database_name = CatalogSettings().gold_database_name  # type: ignore
    source_table = GoldTableNames.gold_measurements
    target_table = f"{source_table}_test_write_stream"
    metering_point_id = random.randint(0, 999999999999999999)
    df_gold = (
        GoldMeasurementsDataFrameBuilder(spark).add_row(metering_point_id=metering_point_id, quality="Bad").build()
    )
    df_gold.write.format("delta").mode("append").saveAsTable(f"{gold_database_name}.{source_table}")
    df_stream_gold = spark.readStream.format("delta").table(f"{gold_database_name}.{source_table}")
    mock_get_checkpoint_path.return_value = "/tmp/checkpoints/start_write_stream_test"
    mock_getenv.return_value = "dbfss://fake_storage_account.blob.core.windows.net"

    # Act
    gold_adapter.start_write_stream(
        df_stream_gold,
        "test_query",
        target_table,
        lambda df, epoch_id: df.write.format("delta")
        .mode("append")
        .saveAsTable(f"{gold_database_name}.{target_table}"),
        True,
    )

    # Assert
    assert (
        spark.read.table(f"{gold_database_name}.{target_table}")
        .filter(f"metering_point_id == '{metering_point_id}'")
        .count()
        == 1
    )


def test__append__should_append_to_gold_table(spark: SparkSession):
    # Arrange
    gold_adapter = DeltaGoldAdapter()
    gold_database_name = CatalogSettings().gold_database_name  # type: ignore
    metering_point_id = random.randint(0, 999999999999999999)
    df_gold = GoldMeasurementsDataFrameBuilder(spark).add_row(metering_point_id=metering_point_id).build()

    # Act
    gold_adapter.append(df_gold, GoldTableNames.gold_measurements)

    # Assert
    assert (
        spark.read.table(f"{gold_database_name}.{GoldTableNames.gold_measurements}")
        .filter(f"metering_point_id == '{metering_point_id}'")
        .count()
        == 1
    )
