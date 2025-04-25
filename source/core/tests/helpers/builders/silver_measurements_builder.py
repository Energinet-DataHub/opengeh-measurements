from decimal import Decimal

from geh_common.domain.types.metering_point_resolution import MeteringPointResolution
from geh_common.domain.types.metering_point_type import MeteringPointType
from geh_common.domain.types.orchestration_type import OrchestrationType
from geh_common.domain.types.quantity_quality import QuantityQuality
from geh_common.domain.types.quantity_unit import QuantityUnit

import tests.helpers.datetime_helper as datetime_helper
from core.silver.domain.schemas.silver_measurements import silver_measurements_schema


class SilverMeasurementsBuilder:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.data = []

    def add_row(
        self,
        orchestration_type=OrchestrationType.SUBMITTED.value,
        orchestration_instance_id="60a518a2-7c7e-4aec-8332",
        metering_point_id="503928175928475638",
        transaction_id="5a76d246-ceae-459f-9e9f",
        transaction_creation_datetime=datetime_helper.get_datetime(year=2020, month=1),
        metering_point_type=MeteringPointType.PRODUCTION.value,
        unit=QuantityUnit.KWH.value,
        resolution=MeteringPointResolution.HOUR.value,
        start_datetime=datetime_helper.get_datetime(year=2020, month=1),
        end_datetime=datetime_helper.get_datetime(year=2020, month=2),
        points=None,
        is_cancelled=False,
        created=datetime_helper.get_datetime(year=2020, month=1, day=1),
    ):
        if points is None:
            points = self._generate_default_points()
        self.data.append(
            (
                orchestration_type,
                orchestration_instance_id,
                metering_point_id,
                transaction_id,
                transaction_creation_datetime,
                metering_point_type,
                unit,
                resolution,
                start_datetime,
                end_datetime,
                points,
                is_cancelled,
                created,
            )
        )
        return self

    def generate_point(self, position: int = 1, quantity: Decimal = Decimal(1.0), quality: str = "measured"):
        return {
            "position": position,
            "quantity": quantity,
            "quality": quality,
        }

    def _generate_default_points(self):
        return [
            {
                "position": position,
                "quantity": Decimal(1.0),
                "quality": QuantityQuality.MEASURED.value,  # Quality is not transformed in the silver layer.
            }
            for position in range(1, 25)
        ]

    def build(self):
        return self.spark.createDataFrame(self.data, schema=silver_measurements_schema)
