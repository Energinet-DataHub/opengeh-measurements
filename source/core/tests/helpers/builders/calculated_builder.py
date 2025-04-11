import random
from datetime import datetime
from decimal import Decimal

from geh_common.data_products.measurements_calculated.calculated_measurements_v1 import schema


class CalculatedMeasurementsBuilder:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.data = []

    def add_row(
        self,
        orchestration_type: str = "capacity_settlement",
        orchestration_instance_id: str = "123456",
        transaction_id="",
        transaction_creation_datetime=datetime.now(),
        metering_point_id: str | None = "502938475674839281",
        metering_point_type: str | None = random.choice(["E17", "E18", "E20", "D01", "D05", "D06", "D07", "D08"]),
        observation_time: datetime | None = datetime.now(),
        quantity: Decimal | None = Decimal(random.uniform(1, 1000)),
        quantity_unit: str = "kWh",
        quantity_quality=random.choice(["measured", "estimated", "calculated", "missing"]),
        resolution: str = "PT1H",
    ):
        self.data.append(
            (
                orchestration_type,
                orchestration_instance_id,
                transaction_id,
                transaction_creation_datetime,
                metering_point_id,
                metering_point_type,
                observation_time,
                quantity,
                quantity_unit,
                quantity_quality,
                resolution,
            )
        )
        return self

    def build(self):
        return self.spark.createDataFrame(self.data, schema=schema)
