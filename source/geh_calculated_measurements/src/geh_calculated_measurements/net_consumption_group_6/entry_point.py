from geh_common.telemetry.decorators import start_trace
from geh_common.telemetry.logger import Logger
from geh_common.telemetry.logging_configuration import LoggingSettings, configure_logging


def execute() -> None:
    logging_settings = LoggingSettings(subsystem="measurements", cloud_role_name="dbr-net-consumption-group-6")
    configure_logging(logging_settings=logging_settings)
    orchestrate_business_logic(logging_settings=logging_settings)


@start_trace()
def orchestrate_business_logic(logging_settings: LoggingSettings) -> None:
    logger = Logger(__name__)
    logger.info(f"Command line arguments / env variables retrieved for Logging Settings: {logging_settings}")
