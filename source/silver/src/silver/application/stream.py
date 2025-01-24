import sys

import telemetry_logging.logging_configuration as config
from opentelemetry.trace import SpanKind
from pyspark.sql import SparkSession
from telemetry_logging import Logger, use_span
from telemetry_logging.span_recording import span_record_exception

import silver.infrastructure.bronze.repository as measurements_bronze_repository
import silver.infrastructure.silver.repository as measurements_silver_repository
from silver.domain.transform_calculated_measurements import transform_calculated_measurements
from silver.infrastructure.utils.spark_initializer import initialize_spark


def execute_calculated_silver_stream(applicationinsights_connection_string: str = None) -> None:
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
            _execute_calculated_silver_stream(spark)
        except Exception as e:
            span_record_exception(e, span)
            sys.exit(4)


@use_span()
def _execute_calculated_silver_stream(spark: SparkSession) -> None:
    bronze_repository = measurements_bronze_repository.Repository(spark)
    silver_repository = measurements_silver_repository.Repository(transform_calculated_measurements)

    log = Logger(__name__)
    log.info("Test that the logger report to appinsights")
    df_bronze_calculated_measurements = bronze_repository.read_calculated_measurements()
    silver_repository.write_measurements(df_bronze_calculated_measurements)
