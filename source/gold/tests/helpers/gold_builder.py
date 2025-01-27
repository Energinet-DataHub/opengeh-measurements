from datetime import datetime
from decimal import Decimal

from gold.domain.schemas.gold_measurements import gold_measurements_schema


class GoldMeasurementsDataFrameBuilder:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.data = []

    def add_row(
        self,
        metering_point_id="502938475674839281",
        observation_time=None,
        quantity=Decimal("0.000"),
        quality="Good",
        metering_point_type="",
        transaction_id="",
        transaction_creation_datetime=None,
        created=None,
        modified=None,
    ):
        self.data.append(
            (
                metering_point_id,
                observation_time or datetime.now(),
                quantity,
                quality,
                metering_point_type,
                transaction_id,
                transaction_creation_datetime or datetime.now(),
                created or datetime.now(),
                modified or datetime.now(),
            )
        )
        return self

    def build(self):
        return self.spark.createDataFrame(self.data, schema=gold_measurements_schema)
