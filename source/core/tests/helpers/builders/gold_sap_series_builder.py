from datetime import datetime

from geh_common.domain.types.metering_point_resolution import MeteringPointResolution
from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from geh_common.domain.types.quantity_unit import QuantityUnit

from core.gold.domain.schemas.gold_measurements_sap_series import gold_measurements_sap_series_schema


class GoldSAPSeriesBuilder:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.data = []

    def add_row(
        self,
        orchestration_type: str = GehCommonOrchestrationType.SUBMITTED.value,
        metering_point_id: str = "502938475674839281",
        transaction_id: str = "123456",
        transaction_creation_datetime: datetime = datetime.now(),
        start_time: datetime = datetime.now(),
        end_time: datetime = datetime.now(),
        unit: str = QuantityUnit.KWH.value,
        resolution: str = MeteringPointResolution.HOUR.value,
        created: datetime = datetime.now(),
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
                created,
            )
        )
        return self

    def build(self):
        return self.spark.createDataFrame(self.data, schema=gold_measurements_sap_series_schema)
