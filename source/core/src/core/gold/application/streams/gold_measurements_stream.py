from pyspark.sql.dataframe import DataFrame

import core.gold.domain.transformations.gold_measurements_transformations as transformations
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository
from core.silver.infrastructure.repositories.silver_measurements_repository import SilverMeasurementsRepository


def stream_measurements_silver_to_gold() -> None:
    silver_measurements = SilverMeasurementsRepository().read_stream()
    GoldMeasurementsRepository().write_stream(
        "measurements", "measurements_silver_to_gold", silver_measurements, _batch_operation
    )


def _batch_operation(silver_measurements: DataFrame, batch_id: int) -> None:
    gold_measurements = transformations.transform_silver_to_gold(silver_measurements)
    GoldMeasurementsRepository().append_if_not_exists(gold_measurements)
