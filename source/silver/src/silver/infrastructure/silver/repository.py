from typing import Callable

from pyspark.sql import DataFrame

from silver.infrastructure.services.env_vars_utils import (
    get_catalog_name,
    get_datalake_storage_account,
)
from silver.infrastructure.services.path_utils import get_checkpoint_path
from silver.infrastructure.silver.database_names import DatabaseNames
from silver.infrastructure.silver.table_names import TableNames


class Repository:
    def __init__(
        self,
        transformer: Callable,
    ) -> None:
        self._transformer = transformer
        self._catalog_name = get_catalog_name()
        self._data_lake_storage_account = get_datalake_storage_account()
        self._checkpoint_path = get_checkpoint_path(
            self._data_lake_storage_account, DatabaseNames.silver_database, TableNames.silver_measurements_table
        )

    def write_measurements(self, df: DataFrame) -> None:
        df.writeStream.queryName("silver_measurements_streaming").option(
            "checkpointLocation", self._checkpoint_path
        ).format("delta").foreachBatch(self._insert_measurements).start().awaitTermination()

    def _insert_measurements(self, df: DataFrame, batchId: int) -> None:
        df = self._transformer(df)
        target_table_name = f"{self._catalog_name + '.' if self._catalog_name else ''}{DatabaseNames.silver_database}.{TableNames.silver_measurements_table}"
        df.write.format("delta").mode("append").saveAsTable(target_table_name)
