from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession

from core.bronze.domain.schemas.migrated_transactions import migrated_transactions_schema


class MigratedTransactionsBuilder:
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
        values: list[tuple[int, str, float]] = [(i, str("D01"), Decimal(i)) for i in range(24)],
        created_in_migrations: datetime = datetime(2025, 2, 10, 13, 0, 0),
        created: datetime = datetime(2025, 2, 10, 14, 0, 0),
    ) -> "MigratedTransactionsBuilder":
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
                created_in_migrations,
                created,
            )
        )

        return self

    def build(self):
        return self.spark.createDataFrame(self.data, schema=migrated_transactions_schema)
