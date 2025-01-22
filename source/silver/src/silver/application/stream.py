"""This module contains the implementation for executing the measurements-silver job."""

import sys

import telemetry_logging.logging_configuration as config
from opentelemetry.trace import SpanKind
from pyspark.sql import SparkSession
from telemetry_logging import use_span, Logger
from telemetry_logging.span_recording import span_record_exception

import silver.infrastructure.bronze.repository as measurements_bronze_repository
import silver.infrastructure.silver.repository as measurements_silver_repository
from silver.infrastructure.environment_variables import (
    get_catalog_name,
    get_datalake_storage_account,
)
from silver.infrastructure.path_helper import get_checkpoint_path
from silver.infrastructure.silver.container_names import ContainerNames
from silver.infrastructure.silver.table_names import TableNames
from silver.infrastructure.spark_initializer import initialize_spark
from silver.application.transform import transform


def execute(
    cloud_role_name: str = "dbr-measurements-silver",
    applicationinsights_connection_string: str | None = None,        
) -> None:    
    """Start overload with explicit dependencies for easier testing."""
    config.configure_logging(
        cloud_role_name=cloud_role_name,
        tracer_name="measurements-silver-job",
        applicationinsights_connection_string=applicationinsights_connection_string,
        extras={"Subsystem": "measurements"},
    )

    with config.get_tracer().start_as_current_span(
        __name__, kind=SpanKind.SERVER
    ) as span:
        try:
            spark = initialize_spark()
            _execute(spark)
        except Exception as e:
            span_record_exception(e, span)
            sys.exit(4)


@use_span()
def _execute(spark: SparkSession) -> None:
    catalog_name = get_catalog_name()
    datalake_storage_account_name = get_datalake_storage_account()
    checkpoint_path = get_checkpoint_path(
        datalake_storage_account_name, 
        ContainerNames.silver_container, 
        TableNames.silver_measurements_table
    )

    bronze_repository = measurements_bronze_repository.Repository(spark, catalog_name)
    silver_repository = measurements_silver_repository.Repository(catalog_name, checkpoint_path, transform)
    
    df_bronze_calculated_measurements = bronze_repository.read_calculated_measurements()
    silver_repository.write_measurements(df_bronze_calculated_measurements, )
    log = Logger(__name__)
    log.info(f"Succesfully wrote calculated measurements to {TableNames.silver_measurements_table}")

