from geh_common.telemetry.decorators import start_trace
from geh_common.telemetry.logger import Logger
from geh_common.telemetry.logging_configuration import LoggingSettings, configure_logging

from geh_calculated_measurements.common.infrastructure.spark_initializor import initialize_spark
from geh_calculated_measurements.net_consumption_group_6.application import execute_application
from geh_calculated_measurements.net_consumption_group_6.application.net_consumption_group_6_args import (
    NetConsumptionGroup6Args,
)


def execute() -> None:
    net_consumption_group_6_args = NetConsumptionGroup6Args()  # Retrieve calculation oriented settings / job arguments
    logging_settings = LoggingSettings(subsystem="measurements", cloud_role_name="dbr-net-consumption-group-6")
    print(f"Logging settings: {logging_settings}")
    configure_logging(logging_settings=logging_settings)
    orchestrate_business_logic(net_consumption_group_6_args, logging_settings=logging_settings)


@start_trace()
def orchestrate_business_logic(job_arguments: NetConsumptionGroup6Args, logging_settings: LoggingSettings) -> None:
    logger = Logger(__name__)
    logger.info(f"Command line arguments / env variables retrieved for Logging Settings: {logging_settings}")
    logger.info(
        f"Command line arguments retrieved for net consumption group 6 job Oriented Parameters: {job_arguments}"
    )
    spark = initialize_spark()
    execute_application(spark, args=job_arguments)
