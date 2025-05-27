import random
from datetime import datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession

import tests.helpers.datetime_helper as datetime_helper
from core.bronze.domain.schemas.submitted_transactions_quarantined import submitted_transactions_quarantined_schema
from core.contracts.process_manager.PersistSubmittedTransaction.decimal_value import DecimalValue
from core.contracts.process_manager.PersistSubmittedTransaction.generated.PersistSubmittedTransaction_pb2 import (
    MeteringPointType,
    OrchestrationType,
    Quality,
    Resolution,
    Unit,
)


class Point:
    def __init__(
        self,
        position: int = 1,
        quantity: DecimalValue | None = DecimalValue(1, 0),
        quality: int = Quality.Q_UNSPECIFIED,
    ) -> None:
        self.position = position
        self.quantity = quantity
        self.quality = quality


class SubmittedTransactionsQuarantinedBuilder:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.data = []

    def add_row(
        self,
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
        points=None,
        created: datetime = datetime_helper.random_datetime(),
        validate_orchestration_type_enum=True,
        validate_quality_enum=True,
        validate_metering_point_type_enum=True,
        validate_unit_enum=True,
        validate_resolution_enum=True,
    ) -> "SubmittedTransactionsQuarantinedBuilder":
        if points is None:
            points = self._generate_default_points()

        self.data.append(
            (
                orchestration_type,
                orchestration_instance_id,
                metering_point_id,
                transaction_id,
                transaction_creation_datetime,
                metering_point_type,
                unit,
                resolution,
                start_datetime,
                end_datetime,
                points,
                created,
                validate_orchestration_type_enum,
                validate_quality_enum,
                validate_metering_point_type_enum,
                validate_unit_enum,
                validate_resolution_enum,
            )
        )

        return self

    def _generate_default_points(self):
        return [
            {
                "position": position,
                "quantity": Decimal(round(random.uniform(0, 1000), 3)),
                "quality": random.choice(["measured", "estimated", "calculated", "missing"]),
            }
            for position in range(1, 25)
        ]

    def build(self) -> DataFrame:
        return self.spark.createDataFrame(self.data, schema=submitted_transactions_quarantined_schema)
