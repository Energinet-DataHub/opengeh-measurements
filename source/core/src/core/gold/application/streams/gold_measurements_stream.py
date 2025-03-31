from dependency_injector.wiring import Provide, inject
from pyspark.sql.dataframe import DataFrame

import core.gold.domain.transformations.gold_measurements_transformations as transformations
from core.containers import Container
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository
from core.silver.infrastructure.repositories.silver_measurements_repository import SilverMeasurementsRepository


@inject
def stream_measurements_silver_to_gold(
    gold_measurements_repository: GoldMeasurementsRepository = Provide[Container.gold_measurements_repository],
    silver_measurements_repository: SilverMeasurementsRepository = Provide[Container.silver_measurements_repository],
) -> None:
    silver_measurements = silver_measurements_repository.read_stream()
    gold_measurements_repository.write_stream(silver_measurements, _batch_operation)


def _batch_operation(silver_measurements: DataFrame, batch_id: int) -> None:
    gold_measurements = transformations.transform_silver_to_gold(silver_measurements)
    GoldMeasurementsRepository().append_if_not_exists(gold_measurements)
