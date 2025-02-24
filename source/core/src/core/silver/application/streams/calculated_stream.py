import sys
from typing import Optional

import geh_common.telemetry.logging_configuration as config
from geh_common.telemetry import use_span
from geh_common.telemetry.span_recording import span_record_exception
from opentelemetry.trace import SpanKind
from pyspark.sql import DataFrame, SparkSession

from core.bronze.infrastructure.streams.bronze_repository import BronzeRepository
from core.settings.catalog_settings import CatalogSettings
from core.silver.application.config.spark_session import initialize_spark
from core.silver.domain.transformations.transform_calculated_measurements import (
    transform_calculated_measurements,
)
from core.silver.infrastructure.config import SilverTableNames
from core.silver.infrastructure.streams import writer
from core.utility.environment_variable_helper import get_datalake_storage_account
from core.utility.shared_helpers import get_checkpoint_path


def execute(applicationinsights_connection_string: Optional[str] = None) -> None:
    """Start overload with explicit dependencies for easier testing."""
    config.configure_logging(
        cloud_role_name="dbr-measurements-silver",
        tracer_name="calculated-measurements-silver-job",
        applicationinsights_connection_string=applicationinsights_connection_string,
        extras={"Subsystem": "measurements"},
    )

    with config.get_tracer().start_as_current_span(__name__, kind=SpanKind.SERVER) as span:
        try:
            spark = initialize_spark()
            _execute(spark)
        except Exception as e:
            span_record_exception(e, span)
            sys.exit(4)


@use_span()
def _execute(spark: SparkSession) -> None:
    catalog_settings = CatalogSettings()  # type: ignore
    bronze_stream = BronzeRepository(spark).read_calculated_measurements()
    data_lake_storage_account = get_datalake_storage_account()
    checkpoint_path = get_checkpoint_path(
        data_lake_storage_account,
        catalog_settings.silver_container_name,
        SilverTableNames.silver_measurements,
    )
    writer.write_stream(
        bronze_stream,
        "bronze_calculated_measurements_to_silver_measurements",
        checkpoint_path,
        _batch_operations,
    )


def _batch_operations(df: DataFrame, batchId: int) -> None:
    catalog_settings = CatalogSettings()  # type: ignore
    df = transform_calculated_measurements(df)
    target_table_name = f"{catalog_settings.silver_database_name}.{SilverTableNames.silver_measurements}"
    df.write.format("delta").mode("append").saveAsTable(target_table_name)
