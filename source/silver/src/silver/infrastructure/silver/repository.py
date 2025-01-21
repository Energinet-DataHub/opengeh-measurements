from tracemalloc import start
from pyspark.sql import DataFrame
from silver.infrastructure.silver.database_names import DatabaseNames
from silver.infrastructure.silver.table_names import TableNames

class Repository:
    def __init__(
        self,
        catalog_name: str,
        checkpoint_path: str,
    ) -> None:
        self._checkpoint_path = checkpoint_path
        self._catalog_name = catalog_name

    def write_measurements(self, df: DataFrame) -> None:
        df_stream_query = df.writeStream \
            .queryName("bronze_to_silver_measurements_streaming") \
            .option("checkpointLocation", self._checkpoint_path) \
            .format("delta") \
            .foreachBatch(self.insert_measurements) \
            .start()
        df_stream_query.awaitTermination()


    def insert_measurements(self, df: DataFrame, batchId: int) -> None:
         df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(f"{self._catalog_name}.{DatabaseNames.silver_database}.{TableNames.silver_measurements_table}")
