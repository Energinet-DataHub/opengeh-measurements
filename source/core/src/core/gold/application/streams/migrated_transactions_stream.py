from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from pyspark.sql import DataFrame

import core.bronze.application.config.spark_session as spark_session
import core.gold.domain.transformations.gold_measurements_transformations as gold_migrations_transformations
import core.silver.domain.transformations.migrations_transformation as silver_migrations_transformations
from core.bronze.infrastructure.repositories.migrated_transactions_repository import (
    MigratedTransactionsRepository,
)
from core.gold.domain.constants.streaming.checkpoint_names import CheckpointNames
from core.gold.domain.constants.streaming.query_names import QueryNames
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository
from core.gold.infrastructure.streams.gold_measurements_stream import GoldMeasurementsStream


def stream_migrated_transactions_to_gold() -> None:
    spark = spark_session.initialize_spark()
    bronze_migrated = MigratedTransactionsRepository(spark).read_stream()
    GoldMeasurementsStream().write_stream(
        CheckpointNames.MIGRATIONS_TO_GOLD.value, QueryNames.MIGRATIONS_TO_GOLD.value, bronze_migrated, _batch_operation
    )


def _batch_operation(batch_df: DataFrame, batch_id: int) -> None:
    bronze_migrated_as_silver = silver_migrations_transformations.transform(batch_df)
    gold_measurements = gold_migrations_transformations.transform_silver_to_gold(bronze_migrated_as_silver)
    GoldMeasurementsRepository().append_if_not_exists(
        gold_measurements, orchestration_type=GehCommonOrchestrationType.MIGRATION
    )
