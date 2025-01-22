import gold.migrations.migrations_runner as migrations_runner
from gold.application.streams.measurements_silver_to_gold.measurements_stream_processor import MeasurementsStreamProcessor
from gold.infrastructure.adapters.delta_gold_repository import DeltaGoldRepository
from gold.infrastructure.adapters.delta_silver_repository import DeltaSilverRepository

from gold.infrastructure.config.table_names import TableNames
from gold.infrastructure.shared_helpers import EnvironmentVariable, get_env_variable_or_throw, initialize_spark


def migrate_gold() -> None:
    migrations_runner.migrate()


def stream_silver_to_gold_measurements() -> None:
    spark = initialize_spark()
    silver_repository = DeltaSilverRepository(spark)
    gold_repository = DeltaGoldRepository()

    silver_source_table = TableNames.silver_measurements_table
    gold_target_table = TableNames.gold_measurements_table
    measurements_stream_processor = MeasurementsStreamProcessor(silver_repository, silver_source_table, gold_repository, gold_target_table)
    measurements_stream_processor.stream_measurements_silver_to_gold()


def get_datalake_storage_account() -> str:
    return get_env_variable_or_throw(EnvironmentVariable.DATALAKE_STORAGE_ACCOUNT)
