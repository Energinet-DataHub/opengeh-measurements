from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

import tests.helpers.datetime_helper as datetime_helper
from tests.helpers.schemas.bronze_submitted_transactions_value_schema import (
    bronze_submitted_transactions_value_schema,
)


class DecimalValue:
    def __init__(self, units: int = 1, nanos: int = 0) -> None:
        self.units = units
        self.nanos = nanos


class Point:
    def __init__(
        self, position: int = 1, quantity: DecimalValue = DecimalValue(1, 0), quality: str = "Q_UNSPECIFIED"
    ) -> None:
        self.position = position
        self.quantity = quantity
        self.quality = quality


class Value:
    def __init__(
        self,
        version: int = 1,
        orchestration_instance_id: str = "60a518a2-7c7e-4aec-8332",
        orchestration_type: str = "OT_UNSPECIFIED",
        metering_point_id: str = "503928175928475638",
        transaction_id: str = "5a76d246-ceae-459f-9e9f",
        transaction_creation_datetime: datetime = datetime_helper.random_datetime(),
        metering_point_type: str = "MPT_UNSPECIFIED",
        unit: str = "U_UNSPECIFIED",
        resolution: str = "R_UNSPECIFIED",
        start_datetime: datetime = datetime_helper.random_datetime(),
        end_datetime: datetime = datetime_helper.random_datetime(),
        points: list[Point] = [Point()],
    ) -> None:
        self.version = version
        self.orchestration_instance_id = orchestration_instance_id
        self.orchestration_type = orchestration_type
        self.metering_point_id = metering_point_id
        self.transaction_id = transaction_id
        self.transaction_creation_datetime = transaction_creation_datetime
        self.metering_point_type = metering_point_type
        self.unit = unit
        self.resolution = resolution
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.points = points


class SubmittedTransactionsValueBuilder:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data = []

    def add_row(
        self,
        value: Value = Value(),
        points: list = [Point()],
    ) -> "SubmittedTransactionsValueBuilder":
        value.points = points
        self.data.append(value)
        return self

    def build(self) -> DataFrame:
        return self.spark.createDataFrame(self.data, schema=bronze_submitted_transactions_value_schema)
