from geh_common.data_products.measurements_core.measurements_gold import current_v1
from geh_common.testing.dataframes import assert_contract
from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.common.domain.model.current_measurements import CurrentMeasurements


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    table_name = current_v1.view_name
    database_name = current_v1.database_name

    def read_current_measurements(self) -> CurrentMeasurements:
        df = self._read()
        assert_contract(df.schema, CurrentMeasurements.schema)
        return CurrentMeasurements(df)

    def _read(self) -> DataFrame:
        """Read table or view. The function is introduced to allow mocking in tests."""
        return self._spark.read.table(f"{self._catalog_name}.{self.database_name}.{self.table_name}")
