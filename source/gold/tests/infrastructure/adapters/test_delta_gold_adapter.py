from pyspark.sql import SparkSession

from gold.infrastructure.adapters.delta_gold_adapter import DeltaGoldAdapter
from gold.infrastructure.config.database_names import DatabaseNames
from gold.infrastructure.config.table_names import TableNames
from tests.helpers.gold_builder import GoldMeasurementsDataFrameBuilder

# @mock.patch("os.getenv")
# @mock.patch("gold.infrastructure.shared_helpers.get_checkpoint_path")
# def test__start_write_stream__should_write_to_gold_table(
#     mock_get_checkpoint_path, mock_get_env_variable, spark: SparkSession
# ):
#     # Arrange
#     gold_adapter = DeltaGoldAdapter()
#     source_table = TableNames.gold_measurements_table
#     target_table = f"{source_table}_test"
#     test_id = "102938275819283728"
#     df_gold = GoldMeasurementsDataFrameBuilder(spark).add_row(metering_point_id=test_id, quality="Bad").build()
#     df_gold.write.format("delta").mode("append").saveAsTable(f"{DatabaseNames.gold_database}.{source_table}")
#     df_stream_gold = spark.readStream.format("delta").table(f"{DatabaseNames.gold_database}.{source_table}")
#
#     # Act
#     gold_adapter.start_write_stream(df_stream_gold, "test_query", target_table, lambda df, _: df)
#
#     # Assert
#     assert (
#         spark.read.table(f"{DatabaseNames.gold_database}.{target_table}")
#         .filter(f"metering_point_id == '{test_id}'")
#         .count()
#         == 1
#     )


def test__append__should_append_to_gold_table(spark: SparkSession):
    # Arrange
    gold_adapter = DeltaGoldAdapter()
    test_id = "102938275819283728"
    df_gold = GoldMeasurementsDataFrameBuilder(spark).add_row(metering_point_id=test_id).build()

    # Act
    gold_adapter.append(df_gold, TableNames.gold_measurements_table)

    # Assert
    assert (
        spark.read.table(f"{DatabaseNames.gold_database}.{TableNames.gold_measurements_table}")
        .filter(f"metering_point_id == '{test_id}'")
        .count()
        == 1
    )
