import gold.migrations.migrations_runner as migrations_runner
from gold.application.streams.silver_to_gold.stream_processor import StreamProcessor
from gold.infrastructure.adapters.delta_gold_writer import DeltaGoldWriter
from gold.infrastructure.adapters.delta_silver_reader import DeltaSilverReader
from gold.infrastructure.config.database_names import DatabaseNames
from gold.infrastructure.config.delta_gold_config import DeltaGoldWriterConfig
from gold.infrastructure.config.delta_silver_config import DeltaSilverReaderConfig
from pyspark import SparkConf
from pyspark.sql.session import SparkSession

from gold.infrastructure.config.table_names import TableNames
from gold.infrastructure.shared_helpers import get_full_table_name, get_checkpoint_path, EnvironmentVariable, \
    get_env_variable_or_throw


def migrate_gold() -> None:
    migrations_runner.migrate()


def stream_silver_to_gold() -> None:
    spark = _initialize_spark()
    silver_reader = _setup_silver_reader(spark)
    gold_writer = _setup_gold_writer()

    stream_processor = StreamProcessor(silver_reader, gold_writer)
    stream_processor.execute_silver_to_gold_stream()


def _initialize_spark() -> SparkSession:
    spark_conf = (
        SparkConf(loadDefaults=True)
        .set("spark.sql.session.timeZone", "UTC")
        .set("spark.sql.shuffle.partitions", "auto")
    )
    return SparkSession.builder.config(conf=spark_conf).getOrCreate()


def _setup_silver_reader(spark: SparkSession) -> DeltaSilverReader:
    silver_reader_options = {"ignoreDeletes": "true"}
    silver_table_full_name = get_full_table_name(DatabaseNames.silver_database, TableNames.silver_measurements_table)
    silver_reader_config = DeltaSilverReaderConfig(silver_table_full_name, silver_reader_options)
    silver_reader_config.validate()
    return DeltaSilverReader(spark, silver_reader_config)


def _setup_gold_writer() -> DeltaGoldWriter:
    full_path_gold = get_full_table_name(DatabaseNames.gold_database, TableNames.gold_measurements_table)
    query_name = "silver_to_gold_streaming"
    checkpoint_location = get_checkpoint_path(get_datalake_storage_account(), DatabaseNames.gold_database, TableNames.gold_measurements_table)
    gold_writer_config = DeltaGoldWriterConfig(full_path_gold, query_name, checkpoint_location)
    gold_writer_config.validate()
    return DeltaGoldWriter(gold_writer_config)


def get_datalake_storage_account() -> str:
    return get_env_variable_or_throw(EnvironmentVariable.DATALAKE_STORAGE_ACCOUNT)
