from google.protobuf.timestamp_pb2 import Timestamp
from pyspark.sql import DataFrame, SparkSession

import tests.helpers.datetime_helper as datetime_helper
from core.bronze.domain.schemas.submitted_transactions import submitted_transactions_schema
from core.contracts.process_manager.PersistSubmittedTransaction.generated.DecimalValue_pb2 import DecimalValue
from core.contracts.process_manager.PersistSubmittedTransaction.generated.PersistSubmittedTransaction_pb2 import (
    MeteringPointType,
    OrchestrationType,
    PersistSubmittedTransaction,
    Point,
    Quality,
    Resolution,
    Unit,
)


class ValueBuilder:
    def build(
        self,
        version: str = "1",
        orchestration_instance_id: str = "60a518a2-7c7e-4aec-8332",
        orchestration_type: OrchestrationType = OrchestrationType.OT_SUBMITTED_MEASURE_DATA,
        metering_point_id: str = "503928175928475638",
        transaction_id: str = "5a76d246-ceae-459f-9e9f",
        transaction_creation_datetime: Timestamp = datetime_helper.get_proto_timestamp(),
        metering_point_type: MeteringPointType = MeteringPointType.MPT_PRODUCTION,
        unit: Unit = Unit.U_KWH,
        resolution: Resolution = Resolution.R_PT15M,
        start_datetime: Timestamp = datetime_helper.get_proto_timestamp(),
        end_datetime: Timestamp = datetime_helper.get_proto_timestamp(),
    ) -> bytes:
        points = [Point(position=1, quantity=DecimalValue(1, 0), quality=Quality.Q_MEASURED)]

        submitted_transaction = PersistSubmittedTransaction(
            version=version,
            orchestration_instance_id=orchestration_instance_id,
            orchestration_type=orchestration_type,
            metering_point_id=metering_point_id,
            transaction_id=transaction_id,
            transaction_creation_datetime=transaction_creation_datetime,
            metering_point_type=metering_point_type,
            unit=unit,
            resolution=resolution,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            points=points,
        )

        return submitted_transaction.SerializeToString()


class SubmittedTransactionsBuilder:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.data = []

    def add_row(
        self,
        key=None,
        value=None,
        topic=None,
        partition=None,
        offset=None,
        timestamp=None,
        timestampType=None,
    ) -> "SubmittedTransactionsBuilder":
        if value is None:
            value = ValueBuilder().build()

        self.data.append((key, value, topic, partition, offset, timestamp, timestampType))

        return self

    def build(self) -> DataFrame:
        return self.spark.createDataFrame(self.data, schema=submitted_transactions_schema)
