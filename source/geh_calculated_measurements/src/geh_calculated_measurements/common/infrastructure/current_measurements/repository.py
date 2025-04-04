from geh_common.testing.dataframes import assert_contract
from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.common.domain.model.current_measurements import CurrentMeasurements
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def read_current_measurements(self) -> CurrentMeasurements:
        df = self._read()
        assert_contract(df.schema, CurrentMeasurements.schema)
        return CurrentMeasurements(df)

    def _read(self) -> DataFrame:
        """Read table or view. The function is introduced to allow mocking in tests."""
        database_name = MeasurementsGoldDatabaseDefinition.DATABASE_NAME
        table_name = MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS
        return self._spark.read.table(f"{self._catalog_name}.{database_name}.{table_name}")
