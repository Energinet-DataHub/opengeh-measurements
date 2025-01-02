import os
import sys
from uuid import UUID

import fire
import telemetry_logging.logging_configuration as config
from opentelemetry.trace import SpanKind
from telemetry_logging.span_recording import span_record_exception

from electrical_heating.domain import calculation
import electrical_heating.infrastructure.environment_variables as env_vars
from electrical_heating.domain.electrical_heating_args import ElectricalHeatingArgs
from electrical_heating.infrastructure.spark_initializor import initialize_spark

CLOUD_ROLE_NAME = "dbr-electrical-heating"
TRACER_NAME = "electrical-heating-job"
SUBSYSTEM = "measurements"


def execute() -> None:
    fire.Fire(_execute)


def _execute(orchestration_instance_id: UUID) -> None:
    args = _create_args(orchestration_instance_id)

    _configure_logging(args)

    with config.get_tracer().start_as_current_span(
        __name__, kind=SpanKind.SERVER
    ) as span:
        # Try/except added to enable adding custom fields to the exception as
        # the span attributes do not appear to be included in the exception.
        try:
            spark = initialize_spark()
            calculation.execute(spark, args)

        except Exception as e:
            span_record_exception(e, span)
            sys.exit(4)


def _create_args(
    orchestration_instance_id: UUID,
) -> ElectricalHeatingArgs:
    return ElectricalHeatingArgs(
        catalog_name=env_vars.get_catalog_name(),
        orchestration_instance_id=orchestration_instance_id,
        time_zone=env_vars.get_time_zone(),
    )


def _configure_logging(args):
    applicationinsights_connection_string = os.getenv(
        "APPLICATIONINSIGHTS_CONNECTION_STRING"
    )

    config.configure_logging(
        cloud_role_name=CLOUD_ROLE_NAME,
        tracer_name=TRACER_NAME,
        applicationinsights_connection_string=applicationinsights_connection_string,
        extras={
            "Subsystem": SUBSYSTEM,
            "orchestration-instance-id": args.orchestration_instance_id,
        },
    )
