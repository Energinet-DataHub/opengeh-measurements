import gold.migrations.migrations_runner as migrations_runner
from gold.application.streams.measurements_silver_to_gold.measurements_stream_processor import MeasurementsStreamProcessor
from gold.infrastructure.adapters.delta_gold_repository import DeltaGoldRepository
from gold.infrastructure.adapters.delta_silver_repository import DeltaSilverRepository
from pyspark import SparkConf
from pyspark.sql.session import SparkSession

from gold.infrastructure.config.table_names import TableNames
from gold.infrastructure.shared_helpers import EnvironmentVariable, get_env_variable_or_throw


def migrate_gold() -> None:
    migrations_runner.migrate()


def stream_silver_to_gold() -> None:
    spark = _initialize_spark()
    silver_repository = DeltaSilverRepository(spark)
    gold_repository = DeltaGoldRepository()

    stream_processor = MeasurementsStreamProcessor(silver_repository, gold_repository)
    silver_source_table = TableNames.silver_measurements_table
    gold_target_table = TableNames.gold_measurements_table
    query_name = "measurements_silver_to_gold"
    stream_processor.stream_measurements_silver_to_gold(silver_source_table, gold_target_table, query_name)


def _initialize_spark() -> SparkSession:
    spark_conf = (
        SparkConf(loadDefaults=True)
        .set("spark.sql.session.timeZone", "UTC")
        .set("spark.sql.shuffle.partitions", "auto")
    )
    return SparkSession.builder.config(conf=spark_conf).getOrCreate()


def get_datalake_storage_account() -> str:
    return get_env_variable_or_throw(EnvironmentVariable.DATALAKE_STORAGE_ACCOUNT)
