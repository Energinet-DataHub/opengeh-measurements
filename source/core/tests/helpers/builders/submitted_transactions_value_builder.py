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
        quantity: DecimalValue | None = DecimalValue(units=1, nanos=0),
        quality: Quality = Quality.Q_MEASURED,
    ) -> "PointsBuilder":
        if quantity:
            self.data.append((position, (quantity.units, quantity.nanos), quality))
        else:
            self.data.append((position, None, quality))
        return self

    def build(self):
        return self.data


class SubmittedTransactionsValueBuilder:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data = []

    def add_row(
        self,
        version: str = "1",
        orchestration_instance_id: str = "60a518a2-7c7e-4aec-8332",
        orchestration_type: OrchestrationType = OrchestrationType.OT_SUBMITTED_MEASURE_DATA,
        metering_point_id: str = "503928175928475638",
        transaction_id: str = "5a76d246-ceae-459f-9e9f",
        transaction_creation_datetime: datetime = datetime_helper.random_datetime(),
        metering_point_type: MeteringPointType = MeteringPointType.MPT_PRODUCTION,
        unit: Unit = Unit.U_KWH,
        resolution: Resolution = Resolution.R_PT15M,
        start_datetime: datetime = datetime_helper.random_datetime(),
        end_datetime: datetime = datetime_helper.random_datetime(),
        points: list[Point] = [Point()],
        key: bytes = b"",
        partition: int = 0,
    ) -> "SubmittedTransactionsValueBuilder":
        self.data.append(
            (
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
                key,
                partition,
            )
        )

        return self

    def build(self) -> DataFrame:
        return self.spark.createDataFrame(self.data, schema=bronze_submitted_transactions_value_schema)
