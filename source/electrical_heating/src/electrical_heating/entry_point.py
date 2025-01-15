from uuid import UUID

from pydantic import Field
from pydantic_settings import BaseSettings

from src.electrical_heating.app_runner import AppRunner, LoggingSettings
from src.electrical_heating.application.app import ElectricalHeatingApp
from src.electrical_heating.application.electrical_heating_args import (
    ElectricalHeatingArgs,
)


class Settings(BaseSettings):
    """
    All settings required to run the application.

    pydantic base settings class that reads and validates
    environment variables and command line arguments.
    """

    # App settings
    orchestration_instance_id: UUID = Field(cli_parse_args=True)
    catalog_name: str = Field(validation_alias="CATALOG_NAME")
    time_zone: str = Field(validation_alias="TIME_ZONE")

    # Logging settings
    applicationinsights_connection_string: str = Field(
        validation_alias="APPLICATIONINSIGHTS_CONNECTION_STRING"
    )
    cloud_role_name = "dbr-electrical-heating"
    subsystem = "electrical-heating"


def execute() -> None:
    settings = Settings()

    app_settings = ElectricalHeatingArgs(
        catalog_name=settings.catalog_name,
        orchestration_instance_id=settings.orchestration_instance_id,
        time_zone=settings.time_zone,
    )

    logging_settings = LoggingSettings(
        cloud_role_name=settings.cloud_role_name,
        applicationinsights_connection_string=settings.applicationinsights_connection_string,
        subsystem=settings.subsystem,
        logging_extras={
            "OrchestrationInstanceId": settings.orchestration_instance_id,
        },
    )

    AppRunner.run(
        ElectricalHeatingApp(app_settings),
        logging_settings,
    )
