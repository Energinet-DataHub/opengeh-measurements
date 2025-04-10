from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

import tests.helpers.datetime_helper as datetime_helper
from core.contracts.process_manager.PersistSubmittedTransaction.generated.DecimalValue_pb2 import DecimalValue
from core.contracts.process_manager.PersistSubmittedTransaction.generated.PersistSubmittedTransaction_pb2 import (
    MeteringPointType,
    OrchestrationType,
    Point,
    Quality,
    Resolution,
    Unit,
)
from tests.helpers.schemas.bronze_submitted_transactions_value_schema import (
    bronze_submitted_transactions_value_schema,
)


class PointsBuilder:
    def __init__(self) -> None:
        self.data = []

    def add_row(
        self,
        position: int = 1,
        quantity: DecimalValue = DecimalValue(units=1, nanos=0),
        quality: Quality = Quality.Q_MEASURED,
    ) -> "PointsBuilder":
        self.data.append(Point(position=position, quantity=quantity, quality=quality))
        return self

    def build(self) -> list[Point]:
        return self.data


class Value:
    def __init__(
        self,
        version: str = "1",
        orchestration_instance_id: str = "60a518a2-7c7e-4aec-8332",
        orchestration_type: int = OrchestrationType.OT_UNSPECIFIED,
        metering_point_id: str = "503928175928475638",
        transaction_id: str = "5a76d246-ceae-459f-9e9f",
        transaction_creation_datetime: datetime = datetime_helper.random_datetime(),
        metering_point_type: int = MeteringPointType.MPT_UNSPECIFIED,
        unit: int = Unit.U_UNSPECIFIED,
        resolution: int = Resolution.R_UNSPECIFIED,
        start_datetime: datetime = datetime_helper.random_datetime(),
        end_datetime: datetime = datetime_helper.random_datetime(),
        points: list[Point] | None = None,
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
        version: str = "1",
        orchestration_instance_id: str = "60a518a2-7c7e-4aec-8332",
        orchestration_type: int = OrchestrationType.OT_SUBMITTED_MEASURE_DATA,
        metering_point_id: str = "503928175928475638",
        transaction_id: str = "5a76d246-ceae-459f-9e9f",
        transaction_creation_datetime: datetime = datetime_helper.random_datetime(),
        metering_point_type: int = MeteringPointType.MPT_PRODUCTION,
        unit: int = Unit.U_KWH,
        resolution: int = Resolution.R_PT15M,
        start_datetime: datetime = datetime_helper.random_datetime(),
        end_datetime: datetime = datetime_helper.random_datetime(),
        points: list[Point] | None = [Point()],
    ) -> "SubmittedTransactionsValueBuilder":
        self.data.append(
            Value(
                version,
                orchestration_instance_id,
                orchestration_type,
                metering_point_id,
                transaction_id,
                transaction_creation_datetime,
                metering_point_type,
                unit,
                resolution,
                start_datetime,
                end_datetime,
                points,
            )
        )

        return self

    def build(self) -> DataFrame:
        return self.spark.createDataFrame(self.data, schema=bronze_submitted_transactions_value_schema)
