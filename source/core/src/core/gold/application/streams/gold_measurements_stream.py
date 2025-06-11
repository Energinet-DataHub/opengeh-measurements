from pyspark.sql.dataframe import DataFrame

import core.gold.domain.transformations.gold_measurements_transformations as transformations
import core.gold.domain.transformations.receipts_transformation as receipt_transformations
import core.gold.domain.transformations.sap_series_transformations as series_sap_transformations
from core.gold.domain.constants.streaming.checkpoint_names import CheckpointNames
from core.gold.domain.constants.streaming.query_names import QueryNames
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository
from core.gold.infrastructure.repositories.measurements_sap_series_repository import GoldMeasurementsSAPSeriesRepository
from core.gold.infrastructure.streams.gold_measurements_stream import GoldMeasurementsStream
from core.receipts.infrastructure.repositories.receipts_repository import ReceiptsRepository
from core.silver.infrastructure.repositories.silver_measurements_repository import SilverMeasurementsRepository


def stream_measurements_silver_to_gold() -> None:
    silver_measurements = SilverMeasurementsRepository().read()
    GoldMeasurementsStream().write_stream(
        CheckpointNames.SILVER_TO_GOLD.value, QueryNames.SILVER_TO_GOLD.value, silver_measurements, _batch_operation
    )


def _batch_operation(silver_measurements: DataFrame, batch_id: int) -> None:
    gold_measurements = transformations.transform_silver_to_gold(silver_measurements)
    GoldMeasurementsRepository().append_if_not_exists(gold_measurements, query_name=QueryNames.SILVER_TO_GOLD)

    receipts = receipt_transformations.transform(gold_measurements)
    ReceiptsRepository().append_if_not_exists(receipts)

    sap_series = series_sap_transformations.transform(silver_measurements)
    GoldMeasurementsSAPSeriesRepository().append_if_not_exists(sap_series)
