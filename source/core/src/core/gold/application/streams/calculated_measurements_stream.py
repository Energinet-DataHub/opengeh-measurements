from pyspark.sql.dataframe import DataFrame

import core.gold.domain.transformations.calculated_measurements_transformations as transformations
from core.gold.domain.constants.streaming.checkpoint_names import CheckpointNames
from core.gold.domain.constants.streaming.query_names import QueryNames
from core.gold.infrastructure.repositories.calculated_measurements_repository import CalculatedMeasurementsRepository
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository


def stream_measurements_calculated_to_gold() -> None:
    calculated_measurements = CalculatedMeasurementsRepository().read_stream()
    GoldMeasurementsRepository().write_stream(
        CheckpointNames.CALCULATED_TO_GOLD.value,
        QueryNames.CALCULATED_TO_GOLD.value,
        calculated_measurements,
        _batch_operation,
    )


def _batch_operation(calculated_measurements: DataFrame, batch_id: int) -> None:
    gold_measurements = transformations.transform_calculated_to_gold(calculated_measurements)
    GoldMeasurementsRepository().append_if_not_exists(gold_measurements)
