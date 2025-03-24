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
from core.contracts.process_manager.enums.metering_point_type import MeteringPointType
from core.contracts.process_manager.enums.orchestration_type import OrchestrationType
from core.contracts.process_manager.enums.quality import Quality
from core.contracts.process_manager.enums.resolution import Resolution
from core.contracts.process_manager.enums.unit import Unit
from core.contracts.process_manager.PersistSubmittedTransaction.decimal_value import DecimalValue
from tests.silver.schemas.bronze_submitted_transactions_value_schema import bronze_submitted_transactions_value_schema


class Point:
    def __init__(self, position: int = 1, quantity=DecimalValue(1, 0), quality: str = Quality.Q_MEASURED.value) -> None:
        self.position = position
        self.quantity = quantity
        self.quality = quality


class PointsBuilder:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.data = []

    def add_row(
        self,
        position: int = 1,
        quantity=DecimalValue(1, 0),
        quality: str = Quality.Q_MEASURED.value,
    ) -> "PointsBuilder":
        self.data.append(Point(position, quantity, quality))
        return self

    def build(self) -> list[Point]:
        return self.data


class Value:
    def __init__(
        self,
        version: str = "1",
        orchestration_instance_id: str = "60a518a2-7c7e-4aec-8332",
        orchestration_type: str = OrchestrationType.OT_SUBMITTED_MEASURE_DATA.value,
        metering_point_id: str = "503928175928475638",
        transaction_id: str = "5a76d246-ceae-459f-9e9f",
        transaction_creation_datetime: datetime = datetime_helper.random_datetime(),
        metering_point_type: str = MeteringPointType.MPT_PRODUCTION.value,
        unit: str = Unit.U_KWH.value,
        resolution: str = Resolution.R_PT15M.value,
        start_datetime: datetime = datetime_helper.random_datetime(),
        end_datetime: datetime = datetime_helper.random_datetime(),
        points: list | None = [Point()],
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


class ValueBuilder:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.data = []

    def add_row(
        self,
        version: str = "1",
        orchestration_instance_id: str = "60a518a2-7c7e-4aec-8332",
        orchestration_type: str = OrchestrationType.OT_SUBMITTED_MEASURE_DATA.value,
        metering_point_id: str = "503928175928475638",
        transaction_id: str = "5a76d246-ceae-459f-9e9f",
        transaction_creation_datetime: datetime = datetime_helper.random_datetime(),
        metering_point_type: str = MeteringPointType.MPT_PRODUCTION.value,
        unit: str = Unit.U_KWH.value,
        resolution: str = Resolution.R_PT15M.value,
        start_datetime: datetime = datetime_helper.random_datetime(),
        end_datetime: datetime = datetime_helper.random_datetime(),
        points: list = [Point()],
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
        version=None,
    ) -> "SubmittedTransactionsBuilder":
        if value is None:
            value = ValueBuilder(self.spark).build()

        self.data.append((key, value, topic, partition, offset, timestamp, timestampType, version))

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
        orchestration_type: str = OrchestrationType.OT_SUBMITTED_MEASURE_DATA.value,
        metering_point_id: str = "503928175928475638",
        transaction_id: str = "5a76d246-ceae-459f-9e9f",
        transaction_creation_datetime: datetime = datetime_helper.random_datetime(),
        metering_point_type: str = MeteringPointType.MPT_PRODUCTION.value,
        unit: str = Unit.U_KWH.value,
        resolution: str = Resolution.R_PT15M.value,
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
