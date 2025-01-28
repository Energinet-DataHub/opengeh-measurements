import sys

import telemetry_logging.logging_configuration as config
from opentelemetry.trace import SpanKind
from pyspark.sql import DataFrame, SparkSession
from telemetry_logging import use_span
from telemetry_logging.span_recording import span_record_exception

from silver.application.config.spark import initialize_spark
from silver.domain.transformations.transform_calculated_measurements import transform_calculated_measurements
from silver.infrastructure.config.container_names import ContainerNames
from silver.infrastructure.config.database_names import DatabaseNames
from silver.infrastructure.config.table_names import TableNames
from silver.infrastructure.helpers.environment_variable_helper import get_datalake_storage_account
from silver.infrastructure.helpers.path_helper import get_checkpoint_path
from silver.infrastructure.streams import writer
from silver.infrastructure.streams.bronze_repository import BronzeRepository


def calculated_stream(applicationinsights_connection_string: str = None) -> None:
    """Start overload with explicit dependencies for easier testing."""
    config.configure_logging(
        cloud_role_name="dbr-measurements-silver",
        tracer_name="measurements-silver-job",
        applicationinsights_connection_string=applicationinsights_connection_string,
        extras={"Subsystem": "measurements"},
    )

    with config.get_tracer().start_as_current_span(__name__, kind=SpanKind.SERVER) as span:
        try:
            spark = initialize_spark()
            _calculated_stream(spark)
        except Exception as e:
            span_record_exception(e, span)
            sys.exit(4)


@use_span()
def _calculated_stream(spark: SparkSession) -> None:
    bronze_stream = BronzeRepository(spark).read_calculated_measurements()
    data_lake_storage_account = get_datalake_storage_account()
    checkpoint_path = get_checkpoint_path(
        data_lake_storage_account, ContainerNames.silver_container, TableNames.silver_measurements_table
    )
    writer.write_stream(
        bronze_stream, "bronze_calculated_measurements_to_silver_measurements", checkpoint_path, _batch_operations
    )


def _batch_operations(df: DataFrame, batchId: int) -> None:
    df = transform_calculated_measurements(df)
    target_table_name = f"{DatabaseNames.silver_database}.{TableNames.silver_measurements_table}"
    df.write.format("delta").mode("append").saveAsTable(target_table_name)
