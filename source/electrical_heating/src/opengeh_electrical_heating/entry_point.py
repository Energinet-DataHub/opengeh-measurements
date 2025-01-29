from telemetry_logging import Logger
from telemetry_logging import logging_configuration as config
from telemetry_logging.decorators import start_trace

from opengeh_electrical_heating.application.execute_with_deps import (
    _execute_with_deps,
)
from opengeh_electrical_heating.application.job_args.electrical_heating_job_args import ElectricalHeatingJobArgs
from opengeh_electrical_heating.infrastructure.spark_initializor import (
    initialize_spark,
)

TRACER_NAME = "electrical-heating-job"


@start_trace  # Wraps the execute_with_deps function that starts the opentelemetry tracer and starts an initial span named using the name of the decorated function
def execute_with_deps() -> None:
    electrical_heating_args = ElectricalHeatingJobArgs()  # Retrieve calculation oriented settings / job arguments
    # Add logging here
    logging_settings = config.LoggingSettings()  # Retrieve logging oriented settings
    # Add logging here
    config.configure_logging(  # Automatically adds the orchestration-instance-id as part of the extras
        logging_settings=logging_settings
    )
    # Add another extra (added to all logging messages as properties)
    config.add_extras({"tracer_name": TRACER_NAME})
    logger = Logger(__name__)
    logger.info(f"Command line arguments / env variables retrieved for Logging Settings: {logging_settings}")
    logger.info(
        f"Command line arguments retrieved for electrical heating job Oriented Parameters: {electrical_heating_args}"
    )

    # span.set_attributes(config.get_extras())
    # args = parse_job_arguments(command_line_args)
    spark = initialize_spark()
    _execute_with_deps(spark, job_arguments=electrical_heating_args, logging_arguments=logging_settings)


def execute() -> None:
    # applicationinsights_connection_string = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
    execute_with_deps()
    # execute_with_deps(
    #     applicationinsights_connection_string=applicationinsights_connection_string,
    # )
