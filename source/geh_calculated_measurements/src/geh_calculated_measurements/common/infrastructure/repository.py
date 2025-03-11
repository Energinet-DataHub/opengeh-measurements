from typing import Any

from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.common.domain import CalculatedMeasurements
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

    def _get_full_table_path(self, table_name: str) -> str:
        database_name = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
        if self._catalog_name:
            return f"{self._catalog_name}.{database_name}.{table_name}"
        return f"{database_name}.{table_name}"

    def _map_data_to_table_and_dataframe(self, data: Any, data_type_to_table_name: dict) -> tuple[str, DataFrame]:
        """Map the input data to the corresponding table name and DataFrame."""
        table_name = data_type_to_table_name.get(type(data))
        if table_name is None:
            raise ValueError(f"Unsupported data type: {type(data)}")

        return table_name, data.df

    def write_to_calculated_measurements_internal_database(
        self, data: Any, data_type_to_table_name: dict, write_mode: str = "append"
    ) -> None:
        """Write the data to the specified Delta table."""
        table_name, df = self._map_data_to_table_and_dataframe(data, data_type_to_table_name)

        if table_name is not None and df is not None:
            df.write.format("delta").mode(write_mode).saveAsTable(self._get_full_table_path(table_name))
        else:
            raise ValueError("Could not determine table name or dataframe from input data.")

    def write_calculated_measurements(self, data: CalculatedMeasurements) -> None:
        df = data.df
        table_name = CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_NAME
        df.write.format("delta").mode("append").saveAsTable(self._get_full_table_path(table_name))
