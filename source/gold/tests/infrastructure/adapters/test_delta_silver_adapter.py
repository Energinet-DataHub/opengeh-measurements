from pyspark.sql import DataFrame, SparkSession

from gold.infrastructure.adapters.delta_silver_adapter import DeltaSilverAdapter
from gold.infrastructure.config.database_names import DatabaseNames
from gold.infrastructure.config.table_names import TableNames
from tests.helpers.silver_builder import SilverMeasurementsDataFrameBuilder


def test__read_stream__should_return_dataframe(spark: SparkSession):
    # Arrange
    silver_adapter = DeltaSilverAdapter(spark)
    table_name = TableNames.silver_measurements_table

    # Act
    result = silver_adapter.read_stream(table_name)

    # Assert
    assert isinstance(result, DataFrame)


def test__read_stream__should_contain_rows_in_silver(spark: SparkSession):
    # Arrange
    silver_adapter = DeltaSilverAdapter(spark)
    table_name = TableNames.silver_measurements_table
    test_table = f"{table_name}_test"
    test_id = "102939999928999"
    df_silver = SilverMeasurementsDataFrameBuilder(spark).add_row(metering_point_id=test_id).build()
    df_silver.write.format("delta").mode("append").saveAsTable(f"{DatabaseNames.silver_database}.{table_name}")

    # Act
    result = silver_adapter.read_stream(table_name)
    result.writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoint").foreachBatch(
        lambda df, epoch_id: df.write.format("delta")
        .mode("append")
        .saveAsTable(f"{DatabaseNames.silver_database}.{test_table}")
    ).trigger(once=True).start().awaitTermination()

    # Assert
    assert (
        spark.read.table(f"{DatabaseNames.silver_database}.{test_table}")
        .filter(f"metering_point_id == '{test_id}'")
        .count()
        == 1
    )
