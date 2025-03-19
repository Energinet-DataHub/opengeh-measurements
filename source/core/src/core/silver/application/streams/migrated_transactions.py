from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from pyspark.sql import DataFrame

import core.bronze.application.config.spark_session as spark_session
import core.silver.domain.transformations.migrations_transformation as migrations_transformation
from core.bronze.infrastructure.repositories.migrated_transactions_repository import (
    MigratedTransactionsRepository,
)
from core.silver.infrastructure.repositories.silver_measurements_repository import (
    SilverMeasurementsRepository,
)


def stream_migrated_transactions_to_silver() -> None:
    spark = spark_session.initialize_spark()
    bronze_migrated_transactions_repository = MigratedTransactionsRepository(spark)
    silver_repository = SilverMeasurementsRepository()

    bronze_migrated = bronze_migrated_transactions_repository.read_measurements_bronze_migrated_transactions_as_stream()

    silver_repository.write_stream(
        measurements=bronze_migrated,
        orchestration_type=GehCommonOrchestrationType.MIGRATION,
        batch_operation=_batch_operation,
    )


def _batch_operation(batch_df: DataFrame, batch_id: int) -> None:
    spark = spark_session.initialize_spark()
    bronze_migrated_as_silver = migrations_transformation.transform(spark, batch_df)
    SilverMeasurementsRepository().append_if_not_exists(silver_measurements=bronze_migrated_as_silver)
