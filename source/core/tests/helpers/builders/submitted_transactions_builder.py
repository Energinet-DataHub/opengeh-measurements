from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.protobuf.functions import to_protobuf

import tests.helpers.datetime_helper as datetime_helper
from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import (
    BronzeSubmittedTransactionsColumnNames,
    ValueColumnNames,
)
from core.bronze.domain.schemas.submitted_transactions import submitted_transactions_schema
from core.contracts.process_manager.descriptor_paths import DescriptorFilePaths
from core.contracts.process_manager.PersistSubmittedTransaction.decimal_value import DecimalValue
from core.contracts.process_manager.PersistSubmittedTransaction.generated.PersistSubmittedTransaction_pb2 import (
    MeteringPointType,
    OrchestrationType,
    Quality,
    Resolution,
    Unit,
)
from tests.helpers.schemas.bronze_submitted_transactions_value_schema import (
    bronze_submitted_transactions_value_schema,
)


class Point:
    def __init__(self, position: int = 1, quantity=DecimalValue(1, 0), quality: Quality = Quality.Q_MEASURED) -> None:
        self.position = position
        self.quantity = quantity
        self.quality = Quality.Name(int(quality))


class PointsBuilder:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.data = []

    def add_row(
        self,
        position: int = 1,
        quantity=DecimalValue(1, 0),
        quality: Quality = Quality.Q_MEASURED,
    ) -> "PointsBuilder":
        self.data.append(Point(position, quantity, quality))
        return self

    @staticmethod
    def generate_point() -> list[Point]:
        return [Point(position, DecimalValue(1, 0), Quality.Q_MEASURED) for position in range(1, 25)]

    def build(self) -> list[Point]:
        return self.data


class Value:
    def __init__(
        self,
        version: str = "1",
        orchestration_instance_id: str = "60a518a2-7c7e-4aec-8332",
        orchestration_type: OrchestrationType = OrchestrationType.OT_SUBMITTED_MEASURE_DATA,
        metering_point_id: str = "503928175928475638",
        transaction_id: str = "5a76d246-ceae-459f-9e9f",
        transaction_creation_datetime: datetime = datetime_helper.random_datetime(),
        metering_point_type: int = MeteringPointType.MPT_PRODUCTION,
        unit: Unit = Unit.U_KWH,
        resolution: Resolution = Resolution.R_PT15M,
        start_datetime: datetime = datetime_helper.get_datetime(year=2020, month=1),
        end_datetime: datetime = datetime_helper.get_datetime(year=2020, month=2),
        points: list | None = PointsBuilder.generate_point(),
    ) -> None:
        self.version = version
        self.orchestration_instance_id = orchestration_instance_id
        self.orchestration_type = OrchestrationType.Name(int(orchestration_type))
        self.metering_point_id = metering_point_id
        self.transaction_id = transaction_id
        self.transaction_creation_datetime = transaction_creation_datetime
        self.metering_point_type = MeteringPointType.Name(int(metering_point_type))
        self.unit = Unit.Name(int(unit))
        self.resolution = Resolution.Name(int(resolution))
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.points = points


class ValueBuilder:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.data = []

    def add_row(
        self,
        version: str = "1",
        orchestration_instance_id: str = "60a518a2-7c7e-4aec-8332",
        orchestration_type: OrchestrationType = OrchestrationType.OT_SUBMITTED_MEASURE_DATA,
        metering_point_id: str = "503928175928475638",
        transaction_id: str = "5a76d246-ceae-459f-9e9f",
        transaction_creation_datetime: datetime = datetime_helper.get_datetime(year=2020, month=1),
        metering_point_type: MeteringPointType = MeteringPointType.MPT_PRODUCTION,
        unit: Unit = Unit.U_KWH,
        resolution: Resolution = Resolution.R_PT15M,
        start_datetime: datetime = datetime_helper.get_datetime(year=2020, month=1),
        end_datetime: datetime = datetime_helper.get_datetime(year=2020, month=2),
        points: list = PointsBuilder.generate_point(),
    ) -> "ValueBuilder":
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

    def build(self):
        if len(self.data) == 0:
            self.data.append(Value())

        df_value = self.spark.createDataFrame(self.data, schema=bronze_submitted_transactions_value_schema)

        df_value = df_value.select(
            F.struct(
                ValueColumnNames.version,
                ValueColumnNames.orchestration_instance_id,
                ValueColumnNames.orchestration_type,
                ValueColumnNames.metering_point_id,
                ValueColumnNames.transaction_id,
                ValueColumnNames.transaction_creation_datetime,
                ValueColumnNames.metering_point_type,
                ValueColumnNames.unit,
                ValueColumnNames.resolution,
                ValueColumnNames.start_datetime,
                ValueColumnNames.end_datetime,
                ValueColumnNames.points,
            ).alias(BronzeSubmittedTransactionsColumnNames.value)
        )

        df_value = df_value.withColumn(
            BronzeSubmittedTransactionsColumnNames.value,
            to_protobuf(
                BronzeSubmittedTransactionsColumnNames.value,
                "PersistSubmittedTransaction",
                descFilePath=DescriptorFilePaths.PersistSubmittedTransaction,
            ),
        )

        return df_value.collect()[0][BronzeSubmittedTransactionsColumnNames.value]


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
            value = ValueBuilder(self.spark).build()

        self.data.append((key, value, topic, partition, offset, timestamp, timestampType))

        return self

    def build(self) -> DataFrame:
        return self.spark.createDataFrame(self.data, schema=submitted_transactions_schema)


class UnpackedSubmittedTransactionsBuilder:
    def __init__(self, spark: SparkSession) -> None:
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
        points: list | None = [Point()],
    ) -> "UnpackedSubmittedTransactionsBuilder":
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
