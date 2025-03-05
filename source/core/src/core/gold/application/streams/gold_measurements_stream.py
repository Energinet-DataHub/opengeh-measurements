from pyspark.sql.dataframe import DataFrame

from core.gold.domain.streams.silver_to_gold.transformations import transform_silver_to_gold
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository
from core.silver.infrastructure.streams.silver_repository import SilverRepository


def stream_measurements_silver_to_gold() -> None:
    silver_measurements = SilverRepository().read()
    GoldMeasurementsRepository().start_write_stream(silver_measurements, pipeline_measurements_silver_to_gold)


def pipeline_measurements_silver_to_gold(df_silver: DataFrame, batch_id: int) -> None:
    df_gold = transform_silver_to_gold(df_silver)
    GoldMeasurementsRepository().append_if_not_exists(df_gold)
