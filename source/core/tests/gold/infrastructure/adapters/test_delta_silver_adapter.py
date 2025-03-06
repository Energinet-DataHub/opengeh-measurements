import random

from pyspark.sql import DataFrame, SparkSession

from core.gold.infrastructure.adapters.delta_silver_adapter import DeltaSilverAdapter
from core.settings.silver_settings import SilverSettings
from core.silver.infrastructure.config import SilverTableNames
from tests.helpers.builders.silver_measurements_builder import SilverMeasurementsBuilder


def test__read_stream__should_return_dataframe(spark: SparkSession, create_silver_tables):
    # Arrange
    silver_adapter = DeltaSilverAdapter(spark)
    table_name = SilverTableNames.silver_measurements

    # Act
    result = silver_adapter.read_stream(table_name)

    # Assert
    assert isinstance(result, DataFrame)


def test__read_stream__should_contain_rows_in_silver(spark: SparkSession):
    # Arrange
    silver_adapter = DeltaSilverAdapter(spark)
    database_name = SilverSettings().silver_database_name
    table_name = SilverTableNames.silver_measurements
    test_table = f"{table_name}_test_read_stream"
    metering_point_id = random.randint(0, 999999999999999999)
    df_silver = SilverMeasurementsBuilder(spark).add_row(metering_point_id=metering_point_id).build()
    df_silver.write.format("delta").mode("append").saveAsTable(f"{database_name}.{table_name}")

    # Act
    result = silver_adapter.read_stream(table_name)
    result.writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoints/read_stream_test").foreachBatch(
        lambda df, epoch_id: df.write.format("delta").mode("append").saveAsTable(f"{database_name}.{test_table}")
    ).trigger(once=True).start().awaitTermination()

    # Assert
    assert (
        spark.read.table(f"{database_name}.{test_table}").filter(f"metering_point_id == '{metering_point_id}'").count()
        == 1
    )
