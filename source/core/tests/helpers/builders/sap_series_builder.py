from datetime import datetime

from core.gold.domain.schemas.gold_measurements_sap_series import sap_series_schema


class SAPSeriesBuilder:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.data = []

    def add_row(
        self,
        orchestration_type: str = "submitted",
        metering_point_id: str | None = "502938475674839281",
        transaction_id: str = "transaction-1",
        transaction_creation_datetime: datetime = datetime.now(),
        start_time: datetime = datetime(2021, 1, 1, 22, 0, 0),
        end_time: datetime = datetime(2021, 1, 2, 22, 0, 0),
        unit: str = "kWh",
        resolution: str = "PT1H",
        created=None,
    ):
        self.data.append(
            (
                orchestration_type,
                metering_point_id,
                transaction_id,
                transaction_creation_datetime,
                start_time,
                end_time,
                unit,
                resolution,
                created or datetime.now(),
            )
        )
        return self

    def build(self):
        return self.spark.createDataFrame(self.data, schema=sap_series_schema)
