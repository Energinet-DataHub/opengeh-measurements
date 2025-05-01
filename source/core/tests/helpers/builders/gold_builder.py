import random
from datetime import datetime, timedelta
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
        quality: str | None = random.choice(["measured", "estimated", "calculated", "missing"]),
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
                quality,
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

    def add_24_hours_rows(
        self,
        metering_point_id: str | None = "502938475674839281",
        orchestration_type: str = "submitted",
        orchestration_instance_id: str = "123456",
        start_time: datetime = datetime.now(),
        metering_point_type: str | None = random.choice(["E17", "E18", "E20", "D01", "D05", "D06", "D07", "D08"]),
        transaction_id="",
        transaction_creation_datetime=datetime.now(),
        resolution: str = "PT1H",
    ):
        for hour in range(24):
            observation_time = start_time + timedelta(hours=hour)
            self.add_row(
                metering_point_id=metering_point_id,
                orchestration_type=orchestration_type,
                orchestration_instance_id=orchestration_instance_id,
                observation_time=observation_time,
                metering_point_type=metering_point_type,
                transaction_id=transaction_id,
                transaction_creation_datetime=transaction_creation_datetime,
                resolution=resolution,
            )
        return self

    def build(self):
        return self.spark.createDataFrame(self.data, schema=gold_measurements_schema)
