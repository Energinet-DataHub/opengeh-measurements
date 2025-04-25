from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

import tests.helpers.identifier_helper as identifier_helper


@dataclass
class MigratedMeasurementsRow:
    metering_point_id: str
    type_of_mp: str
    historical_flag: str
    resolution: str
    transaction_id: str
    transaction_insert_date: datetime
    unit: str
    status: int
    read_reason: str
    valid_from_date: datetime
    valid_to_date: datetime
    values: list[tuple[int, str, Decimal]]
    partitioning_col: date
    created: datetime


class MigratedMeasurementsRowBuilder:
    def build(
        self,
        metering_point_id: str = identifier_helper.create_random_metering_point_id(),
        type_of_mp: str = "E17",
        historical_flag: str = "N",
        resolution: str = "PT1H",
        transaction_id: str = "test_transaction_id",
        transaction_insert_date: datetime = datetime(2023, 1, 1, 12, 0, 0),
        unit: str = "KWH",
        status: int = 2,
        read_reason: str = "",
        valid_from_date: datetime = datetime(2023, 1, 1, 23, 0, 0),
        valid_to_date: datetime = datetime(2023, 1, 2, 23, 0, 0),
        values: list[tuple[int, str, Decimal]] = [(i, "E01", Decimal(i)) for i in range(24)],
        partitioning_col: date = date(2023, 1, 1),
        created: datetime = datetime.now(),
    ) -> MigratedMeasurementsRow:
        return MigratedMeasurementsRow(
            metering_point_id=metering_point_id,
            type_of_mp=type_of_mp,
            historical_flag=historical_flag,
            resolution=resolution,
            transaction_id=transaction_id,
            transaction_insert_date=transaction_insert_date,
            unit=unit,
            status=status,
            read_reason=read_reason,
            valid_from_date=valid_from_date,
            valid_to_date=valid_to_date,
            values=values,
            partitioning_col=partitioning_col,
            created=created,
        )
