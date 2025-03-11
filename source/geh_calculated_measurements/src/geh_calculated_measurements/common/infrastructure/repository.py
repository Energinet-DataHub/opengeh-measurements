from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.capacity_settlement.domain import Calculations, TenLargestQuantities
from geh_calculated_measurements.common.domain import (
    CalculatedMeasurements,
)
from geh_calculated_measurements.common.infrastructure.calculated_measurements.database_definitions import (
    CalculatedMeasurementsInternalDatabaseDefinition,
)


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str | None = None,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    DATA_TYPE_TO_TABLE_NAME = {
        CalculatedMeasurements: CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_NAME,
        Calculations: CalculatedMeasurementsInternalDatabaseDefinition.CAPACITY_SETTLEMENT_CALCULATIONS_NAME,
        TenLargestQuantities: CalculatedMeasurementsInternalDatabaseDefinition.CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES_NAME,
    }

    def _get_full_table_path(self, table_name: str) -> str:
        database_name = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
        if self._catalog_name:
            return f"{self._catalog_name}.{database_name}.{table_name}"
        return f"{database_name}.{table_name}"

    def _map_data_to_table_and_dataframe(
        self, data: CalculatedMeasurements | Calculations | TenLargestQuantities
    ) -> tuple[str, DataFrame]:
        """Map the input data to the corresponding table name and DataFrame."""
        table_name = self.DATA_TYPE_TO_TABLE_NAME.get(type(data))
        if table_name is None:
            raise ValueError(f"Unsupported data type: {type(data)}")

        return table_name, data.df

    def write(
        self, data: CalculatedMeasurements | Calculations | TenLargestQuantities, write_mode: str = "append"
    ) -> None:
        """Write the data to the specified Delta table."""
        table_name, df = self._map_data_to_table_and_dataframe(data)

        if table_name is not None and df is not None:
            df.write.format("delta").mode(write_mode).saveAsTable(self._get_full_table_path(table_name))
        else:
            raise ValueError("Could not determine table name or dataframe from input data.")

    def read(
        self, data_type: type[CalculatedMeasurements | Calculations | TenLargestQuantities]
    ) -> CalculatedMeasurements | Calculations | TenLargestQuantities:
        """Read data from a Delta table."""
        table_name = self.DATA_TYPE_TO_TABLE_NAME.get(data_type)
        if not table_name:
            raise ValueError(f"Unsupported data type: {data_type}")

        df = self._spark.read.table(self._get_full_table_path(table_name))
        return data_type(df)
