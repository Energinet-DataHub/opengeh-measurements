from pyspark.sql import SparkSession

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

    def _get_full_table_path(self, table_name: str) -> str:
        database_name = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
        if self._catalog_name:
            return f"{self._catalog_name}.{database_name}.{table_name}"
        return f"{database_name}.{table_name}"

    def write_calculated_measurements(
        self, calculated_measurements: CalculatedMeasurements, write_mode: str = "append"
    ) -> None:
        calculated_measurements.df.write.format("delta").mode(write_mode).saveAsTable(
            self._get_full_table_path(CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_NAME)
        )

    def read_calculated_measurements(self) -> CalculatedMeasurements:
        return CalculatedMeasurements(
            self._spark.read.table(
                self._get_full_table_path(CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_NAME)
            )
        )

    def write_calculations(self, calculations: Calculations, write_mode: str = "append") -> None:
        calculations.df.write.format("delta").mode(write_mode).saveAsTable(
            self._get_full_table_path(
                CalculatedMeasurementsInternalDatabaseDefinition.CAPACITY_SETTLEMENT_CALCULATIONS_NAME
            )
        )

    def read_calculations(self) -> Calculations:
        return Calculations(
            self._spark.read.table(
                self._get_full_table_path(
                    CalculatedMeasurementsInternalDatabaseDefinition.CAPACITY_SETTLEMENT_CALCULATIONS_NAME
                )
            )
        )

    def write_ten_largest_quantities(
        self, ten_largest_quantities: TenLargestQuantities, write_mode: str = "append"
    ) -> None:
        ten_largest_quantities.df.write.format("delta").mode(write_mode).saveAsTable(
            self._get_full_table_path(
                CalculatedMeasurementsInternalDatabaseDefinition.CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES_NAME
            )
        )

    def read_ten_largest_quantities(self) -> TenLargestQuantities:
        return TenLargestQuantities(
            self._spark.read.table(
                self._get_full_table_path(
                    CalculatedMeasurementsInternalDatabaseDefinition.CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES_NAME
                )
            )
        )
