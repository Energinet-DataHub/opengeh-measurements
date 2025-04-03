import random
from datetime import datetime
from decimal import Decimal

from core.gold.domain.schemas.calculated_measurements import calculated_measurements_schema


class CalculatedMeasurementsBuilder:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.data = []

    def add_row(
        self,
        metering_point_id: str | None = "502938475674839281",
        metering_point_type: str | None = random.choice(["E17", "E18", "E20", "D01", "D05", "D06", "D07", "D08"]),
        observation_time: datetime | None = datetime.now(),
        orchestration_instance_id: str = "123456",
        orchestration_type: str = "capacity_settlement",
        quantity: Decimal | None = Decimal(random.uniform(1, 1000)),
        quantity_quality=random.choice(["measured", "estimated", "calculated", "missing"]),
        quantity_unit: str = "kWh",
        resolution: str = "PT1H",
        transaction_creation_datetime=datetime.now(),
        transaction_id="",
    ):
        self.data.append(
            (
                metering_point_id,
                metering_point_type,
                observation_time,
                orchestration_instance_id,
                orchestration_type,
                quantity,
                quantity_quality,
                quantity_unit,
                resolution,
                transaction_creation_datetime,
                transaction_id,
            )
        )
        return self

    def build(self):
        return self.spark.createDataFrame(self.data, schema=calculated_measurements_schema)
