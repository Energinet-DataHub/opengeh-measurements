import random
from datetime import datetime
from decimal import Decimal

from core.gold.domain.schemas.gold_measurements import gold_measurements_schema


class GoldMeasurementsBuilder:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.data = []

    def add_row(
        self,
        metering_point_id: str | None = "502938475674839281",
        orchestration_type: str = "submitted",
        orchestration_instance_id: str = "123456",
        observation_time: datetime | None = datetime.now(),
        quantity: Decimal | None = Decimal(random.uniform(1, 1000)),
        quality=None,
        metering_point_type: str | None = random.choice(["E17", "E18", "E20", "D01", "D05", "D06", "D07", "D08"]),
        unit: str = "kWh",
        resolution: str = "PT1H",
        transaction_id="",
        transaction_creation_datetime=datetime.now(),
        is_cancelled=False,
        created=None,
        modified=None,
    ):
        self.data.append(
            (
                metering_point_id,
                orchestration_type,
                orchestration_instance_id,
                observation_time,
                quantity,
                quality or random.choice(["measured", "estimated", "calculated", "missing"]),
                metering_point_type,
                unit,
                resolution,
                transaction_id,
                transaction_creation_datetime,
                is_cancelled,
                created or datetime.now(),
                modified or datetime.now(),
            )
        )
        return self

    def build(self):
        return self.spark.createDataFrame(self.data, schema=gold_measurements_schema)
