from datetime import datetime, timezone
from decimal import Decimal

from gold.domain.schemas.gold_measurements import gold_measurements_schema


class GoldMeasurementsDataFrameBuilder:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.data = []

    def add_row(
        self,
        metering_point_id="",
        observation_time=None,
        quantity=Decimal("0.000"),
        quality="",
        metering_point_type="",
        transaction_id="",
        transaction_creation_datetime=None,
        created=None,
        modified=None,
    ):
        """Add a row of data to the builder."""
        self.data.append(
            (
                metering_point_id,
                observation_time or datetime.now(timezone.utc),
                quantity,
                quality,
                metering_point_type,
                transaction_id,
                transaction_creation_datetime or datetime.now(timezone.utc),
                created or datetime.now(timezone.utc),
                modified or datetime.now(timezone.utc),
            )
        )
        return self

    def build(self):
        """Build the DataFrame from the collected data."""
        return self.spark.createDataFrame(self.data, schema=gold_measurements_schema)
