import sys
from pyspark.sql.functions import from_unixtime, unix_timestamp
from pyspark.sql import SparkSession

import telemetry_logging.logging_configuration as config
from opentelemetry.trace import SpanKind
from telemetry_logging.span_recording import span_record_exception
from telemetry_logging import use_span

from source.silver.src.silver.infrastructure.silver.container_names import ContainerNames
from source.silver.src.silver.infrastructure.silver.table_names import TableNames
from source.silver.src.silver.infrastructure.spark_initializer import initialize_spark
from source.silver.src.silver.infrastructure.path_helper import path_helper
from source.silver.src.silver.infrastructure.environment_variables import environment_helper
import source.silver.src.silver.infrastructure.bronze.repository as measurements_bronze_repository
import source.silver.src.silver.infrastructure.silver.repository as measurements_silver_repository

def execute_(
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
    catalog_name = environment_helper.get_catalog_name()
    datalake_storage_account_name = environment_helper.get_data_lake_storage
    checkpoint_path = path_helper.get_checkpoint_path(
        datalake_storage_account_name, 
        ContainerNames.silver_container, 
        TableNames.silver_measurements_table
    )

    bronze_repository = measurements_bronze_repository.Repository(spark, catalog_name)
    silver_repository = measurements_silver_repository.Repository(spark, catalog_name, checkpoint_path)
    
    df_bronze_measurements = bronze_repository.read_measurements()
    silver_repository.write_measurements(df_bronze_measurements)
    