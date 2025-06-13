from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from geh_common.domain.types.metering_point_type import MeteringPointType
from geh_common.domain.types.orchestration_type import OrchestrationType

import tests.helpers.identifier_helper as identifier_helper


@dataclass
class CalculatedMeasurementsRow:
    orchestration_type: str
    orchestration_instance_id: str
    metering_point_id: str
    transaction_id: str
    transaction_creation_datetime: datetime
    transaction_start_time: datetime
    transaction_end_time: datetime
    metering_point_type: str
    observation_time: datetime
    quantity: Decimal


class CalculatedMeasurementsRowBuilder:
    def build(
        self,
        orchestration_type: str = OrchestrationType.CAPACITY_SETTLEMENT.value,
        orchestration_instance_id: str = "test_orchestration_instance_id",
        metering_point_id: str = identifier_helper.create_random_metering_point_id(),
        transaction_id: str = "test_transaction_id",
        transaction_creation_datetime: datetime = datetime(2023, 1, 1, 12, 0, 0),
        transaction_start_time: datetime = datetime(2023, 1, 1, 12, 0, 0),
        transaction_end_time: datetime = datetime(2023, 1, 2, 12, 0, 0),
        metering_point_type: str = MeteringPointType.CAPACITY_SETTLEMENT.value,
        observation_time: datetime = datetime(2023, 1, 1, 12, 0, 0),
        quantity: Decimal = Decimal("123.45"),
    ) -> CalculatedMeasurementsRow:
        return CalculatedMeasurementsRow(
            orchestration_type=orchestration_type,
            orchestration_instance_id=orchestration_instance_id,
            metering_point_id=metering_point_id,
            transaction_id=transaction_id,
            transaction_creation_datetime=transaction_creation_datetime,
            transaction_start_time=transaction_start_time,
            transaction_end_time=transaction_end_time,
            metering_point_type=metering_point_type,
            observation_time=observation_time,
            quantity=quantity,
        )
