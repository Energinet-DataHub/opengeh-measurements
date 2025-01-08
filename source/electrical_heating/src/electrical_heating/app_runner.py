"""
The content of this module should be in a package in `opengeh-python-packages`. Possibly named `opengeh-app`.
"""

﻿﻿from dataclasses import dataclass

import telemetry_logging.logging_configuration as config
from opentelemetry.trace import SpanKind


class AppMeta(type):
    """
    An app metaclass that is used (implicitly) when instantiating classes implementing the class `AppInterface`.
    """

    def __instancecheck__(cls, instance):
        return cls.__subclasscheck__(type(instance))

    def __subclasscheck__(cls, subclass):
        return hasattr(subclass, "run") and callable(subclass.run)


class AppInterface(metaclass=AppMeta):
    """Base class of all applications"""

    def run(self):
        """All classes implementing this interface must implement the run method."""
        pass


# TODO: Move to telemetry package
@dataclass
class LoggingSettings:
    cloud_role_name: str
    applicationinsights_connection_string: str
    subsystem: str
    logging_extras: dict = None


class AppRunner:
    @classmethod
    def run(
        cls,
        app: AppInterface,
        logging_settings: LoggingSettings,
    ):
        """The runner sets up logging and runs the application."""

        if not isinstance(app, AppInterface):
            raise TypeError("'app' must be a subclass of 'AppInterface'")

        _run(app, logging_settings)


def _run(
    app: AppInterface,
    logging_settings: LoggingSettings,
) -> None:
    # TODO: Add configure_logging(logging_settings) function to telemetry package
    config.configure_logging(
        cloud_role_name=logging_settings.cloud_role_name,
        tracer_name=logging_settings.subsystem,
        applicationinsights_connection_string=logging_settings.applicationinsights_connection_string,
        extras=logging_settings.logging_extras,
    )

    with config.get_tracer().start_as_current_span(__name__, kind=SpanKind.SERVER):
        app.run()
