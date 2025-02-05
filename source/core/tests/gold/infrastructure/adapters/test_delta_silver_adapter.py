import random

from helpers.silver_builder import SilverMeasurementsDataFrameBuilder
from pyspark.sql import DataFrame, SparkSession

from src.core.gold.infrastructure.adapters.delta_silver_adapter import DeltaSilverAdapter
from src.core.gold.infrastructure.config.database_names import DatabaseNames
from src.core.gold.infrastructure.config.table_names import TableNames


def test__read_stream__should_return_dataframe(spark: SparkSession, create_silver_tables):
    # Arrange
    silver_adapter = DeltaSilverAdapter(spark)
    table_name = TableNames.silver_measurements

    # Act
    result = silver_adapter.read_stream(table_name)

    # Assert
    assert isinstance(result, DataFrame)


def test__read_stream__should_contain_rows_in_silver(spark: SparkSession, create_silver_tables):
    # Arrange
    silver_adapter = DeltaSilverAdapter(spark)
    table_name = TableNames.silver_measurements
    test_table = f"{table_name}_test"
    metering_point_id = random.randint(0, 999999999999999999)
    df_silver = SilverMeasurementsDataFrameBuilder(spark).add_row(metering_point_id=metering_point_id).build()
    df_silver.write.format("delta").mode("append").saveAsTable(f"{DatabaseNames.silver}.{table_name}")

    # Act
    result = silver_adapter.read_stream(table_name)
    result.writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoints/read_stream_test").foreachBatch(
        lambda df, epoch_id: df.write.format("delta").mode("append").saveAsTable(f"{DatabaseNames.silver}.{test_table}")
    ).trigger(once=True).start().awaitTermination()

    # Assert
    assert (
        spark.read.table(f"{DatabaseNames.silver}.{test_table}")
        .filter(f"metering_point_id == '{metering_point_id}'")
        .count()
        == 1
    )
