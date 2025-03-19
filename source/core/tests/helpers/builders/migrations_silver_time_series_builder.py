from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession

from core.bronze.domain.schemas.migrated_transactions import migrations_silver_time_series_schema


class MigrationsSilverTimeSeriesBuilder:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data = []

    def add_row(
        self,
        metering_point_id: str = "570000000000000000",
        type_of_mp: str = "E17",
        historical_flag: str = "N",
        resolution: str = "PT1H",
        transaction_id: str = "UnitTestingTransaction001",
        transaction_insert_date: datetime = datetime(2025, 2, 10, 12, 0, 0),
        unit: str = "KWH",
        status: int = 0,
        read_reason: str = "",
        valid_from_date: datetime = datetime(2025, 2, 8, 23, 0, 0),
        valid_to_date: datetime = datetime(2025, 2, 9, 23, 0, 0),
        values: list[tuple[int, str, Decimal]] = [(i, "D01", Decimal(i)) for i in range(24)],
        partitioning_col: datetime = datetime(2025, 2, 10),
        created: datetime = datetime(2025, 2, 10, 13, 0, 0),
    ) -> "MigrationsSilverTimeSeriesBuilder":
        self.data.append(
            (
                metering_point_id,
                type_of_mp,
                historical_flag,
                resolution,
                transaction_id,
                transaction_insert_date,
                unit,
                status,
                read_reason,
                valid_from_date,
                valid_to_date,
                values,
                partitioning_col,
                created,
            )
        )

        return self

    def build(self):
        return self.spark.createDataFrame(self.data, schema=migrations_silver_time_series_schema)
