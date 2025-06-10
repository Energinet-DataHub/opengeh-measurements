from pyspark.sql import DataFrame

import core.bronze.application.config.spark_session as spark_session
import core.gold.domain.transformations.sap_series_transformations as series_sap_transformations
import core.silver.domain.filters.migrations_filters as silver_migrations_filters
import core.silver.domain.transformations.migrations_transformation as silver_migrations_transformations
from core.bronze.infrastructure.repositories.migrated_transactions_repository import (
    MigratedTransactionsRepository,
)
from core.gold.domain.constants.streaming.checkpoint_names import CheckpointNames
from core.gold.domain.constants.streaming.query_names import QueryNames
from core.gold.infrastructure.repositories.measurements_sap_series_repository import GoldMeasurementsSAPSeriesRepository
from core.gold.infrastructure.streams.gold_measurements_stream import GoldMeasurementsStream


def stream_migrated_transactions_to_sap_series_gold() -> None:
    spark = spark_session.initialize_spark()
    bronze_migrated = MigratedTransactionsRepository(spark).read_stream()
    GoldMeasurementsStream().write_stream(
        CheckpointNames.MIGRATIONS_TO_GOLD_SAP_SERIES.value,
        QueryNames.MIGRATIONS_TO_SAP_SERIES_GOLD.value,
        bronze_migrated,
        _batch_operation,
    )


def _batch_operation(batch_df: DataFrame, batch_id: int) -> None:
    batch_df_filtered = silver_migrations_filters.filter_away_rows_older_than_2017(batch_df)
    bronze_migrated_as_silver = silver_migrations_transformations.transform(batch_df_filtered)

    sap_series = series_sap_transformations.transform(bronze_migrated_as_silver)
    GoldMeasurementsSAPSeriesRepository().append_if_not_exists(sap_series)
