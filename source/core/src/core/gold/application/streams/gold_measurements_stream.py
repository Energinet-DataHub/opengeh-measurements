from pyspark.sql.dataframe import DataFrame

import core.gold.domain.streams.silver_to_gold.transformations as transformations
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository
from core.silver.infrastructure.streams.silver_repository import SilverRepository


def stream_measurements_silver_to_gold() -> None:
    silver_measurements = SilverRepository().read()
    GoldMeasurementsRepository().start_write_stream(silver_measurements, pipeline_measurements_silver_to_gold)


def pipeline_measurements_silver_to_gold(silver_measurements: DataFrame, batch_id: int) -> None:
    gold_measurements = transformations.transform_silver_to_gold(silver_measurements)
    GoldMeasurementsRepository().append_if_not_exists(gold_measurements)
