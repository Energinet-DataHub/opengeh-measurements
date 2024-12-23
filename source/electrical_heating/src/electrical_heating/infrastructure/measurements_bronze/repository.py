from pyspark.sql import DataFrame

from source.electrical_heating.src.electrical_heating.infrastructure.measurements_bronze.database_definitions import (
    MeasurementsBronzeDatabase,
)


class Repository:
    def __init__(
        self,
        catalog_name: str,
    ) -> None:
        self._catalog_name = catalog_name

    def write_measurements(self, df: DataFrame) -> None:
        df.writeTo(
            f"{self._catalog_name}.{MeasurementsBronzeDatabase.DATABASE_NAME}.{MeasurementsBronzeDatabase.MEASUREMENTS_NAME}"
        )
