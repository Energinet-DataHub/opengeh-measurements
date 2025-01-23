import random
from datetime import datetime, timezone
from decimal import Decimal

from silver.domain.schemas.silver_measurements import silver_measurements_schema


class SilverMeasurementsDataFrameBuilder:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.data = []

    def add_row(
        self,
        orchestration_type="",
        orchestration_instance_id="",
        metering_point_id="",
        transaction_id="",
        transaction_creation_datetime=None,
        metering_point_type="",
        product="",
        unit="",
        resolution="",
        start_datetime=None,
        end_datetime=None,
        points=None,
        created=None,
    ):
        if points is None:
            points = self._generate_default_points()
        self.data.append(
            (
                orchestration_type,
                orchestration_instance_id,
                metering_point_id,
                transaction_id,
                transaction_creation_datetime or datetime.utcnow(),
                metering_point_type,
                product,
                unit,
                resolution,
                start_datetime or datetime.utcnow(),
                end_datetime or datetime.utcnow(),
                points,
                created or datetime.utcnow(),
            )
        )
        return self

    def _generate_default_points(self):
        return [
            {
                "position": position,
                "quantity": Decimal(round(random.uniform(0, 1000), 3)),
                "quality": random.choice(["High", "Medium", "Low"]),
            }
            for position in range(1, 25)
        ]

    def build(self):
        return self.spark.createDataFrame(self.data, schema=silver_measurements_schema)
