"""Entry point for the capacity-settlement application."""

from telemetry_logging import logging_configuration
from telemetry_logging.decorators import start_trace
from telemetry_logging.logger import Logger

from opengeh_capacity_settlement.application.execute_with_deps import _execute_with_deps
from opengeh_capacity_settlement.application.job_args.capacity_settlement_args import CapacitySettlementArgs

TRACER_NAME = "capacity-settlement-job"


def execute() -> None:
    electrical_heating_args = CapacitySettlementArgs()  # Retrieve calculation oriented settings / job arguments
    logging_settings = logging_configuration.LoggingSettings()  # Retrieve logging oriented settings
    logging_settings.force_configuration = True
    logging_configuration.configure_logging(  # Automatically adds the orchestration-instance-id as part of the extras
        logging_settings=logging_settings, extras={"chba": "chba debug value"}
    )
    print(logging_settings)  # noqa: T201
    print(logging_configuration._IS_INSTRUMENTED)  # noqa: T201
    print(logging_configuration._LOGGING_CONFIGURED)  # noqa: T201
    # Add another extra (added to all logging messages as properties)
    logging_configuration.add_extras({"chba2": "chba debug value"})
    # Execute the application
    orchestrate_business_logic(job_arguments=electrical_heating_args, logging_settings=logging_settings)


@start_trace(
    initial_span_name="capacity_settlement.orchestrate_business_logic"
)  # Wraps the execute_with_deps function that starts the opentelemetry tracer and starts an initial span named using the name of the decorated function, or specifically provided name
def orchestrate_business_logic(
    job_arguments: CapacitySettlementArgs, logging_settings: logging_configuration.LoggingSettings
) -> None:
    logger = Logger(__name__)
    logger.info(f"Command line arguments / env variables retrieved for Logging Settings: {logging_settings}")
    logger.info(f"Command line arguments retrieved for electrical heating job Oriented Parameters: {job_arguments}")
    _execute_with_deps(job_arguments=job_arguments)
